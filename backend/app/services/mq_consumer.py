"""消息队列消费领域服务：消费组管理、偏移追踪、消息拉取与死信投递。

提供消息消费的完整生命周期管理，包括消费组注册、批量拉取、偏移提交、
超时处理以及失败消息的死信队列路由。

消费进度模型（随消费配置记录持久化，重启后可恢复）：
- 消费偏移确认明细 offset_confirmations：逐偏移记录真实处置结果
  （consumed 消费成功 / dlq_registered 已入死信），死信登记凭证随明细
  回写消费清单；
- 可重取的偏移 pending_offsets：处理失败且死信写入也失败的偏移，不进入
  确认明细、不提交消费结果，保留在原偏移上等待续做；
- 续作接续点 current_offset：最小的未确认偏移，中断/重启后由此继续消费，
  与死信清单覆盖范围保持一致。

跨模块关联：dlq_handler（死信路由）、call_trace_log（消费追踪）。
"""
from __future__ import annotations

import hashlib
import json
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class MqConsumerService(BaseModuleService):
    MODULE = "mq_consumer"
    ENTITY = "消费配置"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["配置编号", "队列名称", "消费组名", "偏移策略", "批量大小", "超时时间", "死信队列", "消费状态"]
    REQUIRED_FIELDS = ["配置编号", "队列名称", "消费组名"]
    STATUS_ORDER = ["草稿", "待校验", "消费中", "已停止"]
    ACTION_RULES = {"提交校验": "待校验", "启动消费": "消费中", "停止消费": "已停止"}
    NEGATIVE_ACTIONS = []

    # 进度写入锁：类级共享，保证并发续做时同一原偏移只确认一次
    _PROGRESS_LOCK = threading.Lock()

    def __init__(self) -> None:
        super().__init__()
        self._offsets: dict[str, int] = {}
        self._consumed_messages: list[dict[str, Any]] = []

    def register_consumer(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """注册新的消费者配置。"""
        return self.create_entry(values)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """创建消费配置，附带初始偏移和消费组注册。"""
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing

        existing = json_store.get_all(self.MODULE)
        max_id = max((int(e.get("id", 0)) for e in existing), default=0)
        entry = {"id": max_id + 1}
        entry.update({field: values.get(field, "") for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["created_at"] = datetime.now(timezone.utc).isoformat()
        entry["current_offset"] = 0
        entry["messages_consumed"] = 0
        entry["offset_confirmations"] = []  # 消费偏移确认明细
        entry["pending_offsets"] = []  # 可重取的偏移（处理失败且未入死信）
        entry["last_commit_at"] = None

        json_store.insert(self.MODULE, entry)

        # 初始化偏移
        queue_name = str(values.get("队列名称", ""))
        group_name = str(values.get("消费组名", ""))
        offset_key = f"{queue_name}:{group_name}"
        self._offsets[offset_key] = 0

        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """状态流转：同步消费入口状态到消费进度存储，保证消费链路状态机一致。"""
        entry, message = super().run_action(entry_id, action)
        target = self.ACTION_RULES.get(action)
        if target is not None and json_store.get_by_id(self.MODULE, entry_id) is not None:
            json_store.update(self.MODULE, entry_id, {
                "status": target,
                "pending": target != self.STATUS_ORDER[-1],
            })
        return entry, message

    def _load_progress(self, consumer: dict[str, Any]) -> dict[str, Any]:
        """加载消费进度：确认明细、可重取偏移与续作接续点。

        兼容历史偏移记录（无确认明细字段时按 current_offset 续作）；
        对死信登记类确认按登记凭证校验，死信记录缺失的偏移降级为可重取，
        恢复时能重新定位丢失的消息。
        """
        raw_confirmations = consumer.get("offset_confirmations")
        confirmations: list[dict[str, Any]] = []
        if isinstance(raw_confirmations, list):
            confirmations = [
                c for c in raw_confirmations
                if isinstance(c, dict) and c.get("offset") is not None
            ]

        # 根据凭证校验进度：死信登记确认必须能关联到真实死信记录
        dlq_ids = {int(r.get("id", 0)) for r in json_store.get_all("dlq_handler")}
        valid_confirmations: list[dict[str, Any]] = []
        recovered: set[int] = set()
        for conf in confirmations:
            if conf.get("result") == "dlq_registered":
                dlq_id = conf.get("dlq_id")
                try:
                    credential_valid = dlq_id is not None and int(dlq_id) in dlq_ids
                except (TypeError, ValueError):
                    credential_valid = False
                if not credential_valid:
                    # 死信记录缺失：原消息重新定位，回到可重取范围
                    recovered.add(int(conf["offset"]))
                    continue
            valid_confirmations.append(conf)

        confirmed = {int(c["offset"]) for c in valid_confirmations}
        pending: set[int] = set()
        raw_pending = consumer.get("pending_offsets")
        if isinstance(raw_pending, list):
            for item in raw_pending:
                try:
                    pending.add(int(item))
                except (TypeError, ValueError):
                    continue
        pending = (pending | recovered) - confirmed

        if pending:
            resume = min(pending)
        elif confirmed:
            resume = max(confirmed) + 1
        else:
            try:
                resume = int(consumer.get("current_offset", 0) or 0)
            except (TypeError, ValueError):
                resume = 0

        return {
            "confirmations": valid_confirmations,
            "confirmed": confirmed,
            "pending": pending,
            "recovered": recovered,
            "resume": resume,
        }

    def _apply_dispositions(self, consumer_id: int, offset_key: str,
                            dispositions: list[dict[str, Any]]) -> dict[str, Any]:
        """把本批次处置结果写入有效消费进度（消费偏移确认明细）。

        - 消费成功 / 死信登记成功的偏移进入确认明细，登记凭证随明细回写消费清单；
        - 死信写入失败的偏移保留为可重取偏移，不进入确认明细，防止原消息丢失；
        - 已确认偏移只确认一次（并发续做幂等），不重复确认、不重复计数。
        """
        now = datetime.now(timezone.utc).isoformat()
        with self._PROGRESS_LOCK:
            consumer = json_store.get_by_id(self.MODULE, consumer_id) or {}
            progress = self._load_progress(consumer)
            confirmations = list(progress["confirmations"])
            confirmed = set(progress["confirmed"])
            pending = set(progress["pending"])

            consumed = 0
            dlq_registered = 0
            unresolved = 0
            skipped = 0
            for disp in dispositions:
                offset = int(disp["offset"])
                if offset in confirmed:
                    # 并发续做：已完成的原偏移只确认一次
                    skipped += 1
                    continue
                if disp.get("result") == "pending":
                    pending.add(offset)
                    unresolved += 1
                    continue
                confirmations.append({
                    "offset": offset,
                    "result": disp.get("result", "consumed"),
                    "credential": str(disp.get("credential") or ""),
                    "dlq_id": disp.get("dlq_id"),
                    "confirmed_at": now,
                })
                confirmed.add(offset)
                pending.discard(offset)
                if disp.get("result") == "dlq_registered":
                    dlq_registered += 1
                else:
                    consumed += 1

            if pending:
                resume = min(pending)
            elif confirmed:
                resume = max(confirmed) + 1
            else:
                try:
                    resume = int(consumer.get("current_offset", 0) or 0)
                except (TypeError, ValueError):
                    resume = 0

            newly_confirmed = consumed + dlq_registered
            try:
                base_consumed = int(consumer.get("messages_consumed", 0) or 0)
            except (TypeError, ValueError):
                base_consumed = 0

            self._offsets[offset_key] = resume
            json_store.update(self.MODULE, consumer_id, {
                "current_offset": resume,
                "offset_confirmations": confirmations,
                "pending_offsets": sorted(pending),
                "messages_consumed": base_consumed + newly_confirmed,
                "last_commit_at": now,
            })

        return {
            "consumed": consumed,
            "dlq_registered": dlq_registered,
            "unresolved": unresolved,
            "skipped_confirmed": skipped,
            "failed": dlq_registered + unresolved,
            "resume": resume,
            "pending_offsets": sorted(pending),
            "recovered": sorted(progress["recovered"]),
        }

    def consume_batch(self, consumer_id: int, max_messages: int = 10) -> dict[str, Any]:
        """从模拟消息队列中批量消费消息。

        流程：
        1. 检查消费者状态是否为"消费中"
        2. 按消费进度定位续作接续点，仅拉取未确认偏移的消息
        3. 逐条消费并校验
        4. 消费失败的消息路由到死信队列，登记凭证随确认明细回写消费清单；
           死信写入失败的偏移保留为可重取偏移，不提交确认
        5. 提交有效消费进度（确认明细 + 可重取偏移 + 接续点）

        返回消费结果包含成功/失败消息明细。
        """
        consumer = json_store.get_by_id(self.MODULE, consumer_id)
        if consumer is None:
            return {"ok": False, "message": f"消费者 {consumer_id} 不存在", "consumed": 0, "failed": 0}

        if consumer.get("status") != "消费中":
            return {"ok": False, "message": f"消费者未启动（当前状态: {consumer['status']}）", "consumed": 0, "failed": 0}

        queue_name = str(consumer.get("队列名称", "default"))
        group_name = str(consumer.get("消费组名", "default"))
        offset_key = f"{queue_name}:{group_name}"
        try:
            batch_size = int(consumer.get("批量大小", "") or max_messages)
        except (TypeError, ValueError):
            batch_size = max_messages

        progress = self._load_progress(consumer)
        resume_offset = progress["resume"]
        timeout_ms = int(str(consumer.get("超时时间", "5000") or "5000").split("T")[0] if "T" in str(consumer.get("超时时间", "")) else str(consumer.get("超时时间", "5000") or "5000"))

        # 模拟消息生成：从接续点起仅拉取未确认偏移，已确认偏移保持原消费结果
        messages = []
        candidate = resume_offset
        target = min(batch_size, max_messages)
        while len(messages) < target:
            if candidate not in progress["confirmed"]:
                messages.append({
                    "offset": candidate,
                    "message_id": str(uuid.uuid4()),
                    "queue": queue_name,
                    "group": group_name,
                    "payload": {
                        "type": "data_event",
                        "data": {
                            "id": candidate + 1,
                            "value": f"record_{candidate + 1}",
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                    },
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
            candidate += 1

        dispositions: list[dict[str, Any]] = []
        dlq_records = []

        import random
        for msg in messages:
            # 模拟处理：20%概率失败
            if random.random() < 0.2:
                # 路由到死信队列（跨模块：dlq_handler）
                dlq_record = self._route_to_dlq(consumer_id, msg, "处理超时" if len(dlq_records) % 3 == 0 else "数据格式异常")
                dlq_records.append(dlq_record)
                if dlq_record.get("ok"):
                    # 死信登记成功：登记凭证关联到消费偏移确认明细
                    dispositions.append({
                        "offset": msg["offset"],
                        "result": "dlq_registered",
                        "credential": dlq_record.get("record_no", ""),
                        "dlq_id": dlq_record.get("dlq_id"),
                    })
                else:
                    # 死信写入失败：偏移保留为可重取，不进入确认明细
                    dispositions.append({"offset": msg["offset"], "result": "pending"})
            else:
                # 记录消费追踪（跨模块：call_trace_log）
                trace_credential = self._record_consumption(consumer_id, msg, timeout_ms)
                dispositions.append({
                    "offset": msg["offset"],
                    "result": "consumed",
                    "credential": trace_credential,
                })

        # 提交有效消费进度：确认明细反映真实处置结果
        outcome = self._apply_dispositions(consumer_id, offset_key, dispositions)

        return {
            "ok": True,
            "message": f"消费完成: 成功 {outcome['consumed']}, 失败 {outcome['failed']}",
            "consumed": outcome["consumed"],
            "failed": outcome["failed"],
            "dlq_registered": outcome["dlq_registered"],
            "unresolved": outcome["unresolved"],
            "skipped_confirmed": outcome["skipped_confirmed"],
            "total": len(messages),
            "dlq_records": len(dlq_records),
            "offsets": [m["offset"] for m in messages],
            "messages": [
                {"offset": m["offset"], "message_id": m["message_id"], "payload": m["payload"]}
                for m in messages
            ],
            "previous_offset": resume_offset,
            "new_offset": outcome["resume"],
            "pending_offsets": outcome["pending_offsets"],
        }

    def _route_to_dlq(self, consumer_id: int, message: dict[str, Any], reason: str) -> dict[str, Any]:
        """将消费失败的消息路由到死信队列。"""
        try:
            from app.services.dlq_handler import DlqHandlerService
            dlq_svc = DlqHandlerService()
            payload_str = json.dumps(message.get("payload", {}), ensure_ascii=False)
            return dlq_svc.enqueue_dead_letter(
                source_queue=str(message.get("queue", "unknown")),
                original_message=payload_str,
                failure_reason=reason,
                consumer_id=consumer_id,
                offset=int(message.get("offset", -1)),
            )
        except Exception as e:
            return {"ok": False, "reason": str(e)}

    def _record_consumption(self, consumer_id: int, message: dict[str, Any], latency_ms: int) -> str:
        """记录消费成功的追踪信息，返回追踪凭证用于确认明细登记。"""
        try:
            from app.services.call_trace_log import CallTraceLogService
            tracer = CallTraceLogService()
            result = tracer._record_trace(
                endpoint_id=consumer_id,
                action="消息消费",
                latency_ms=min(latency_ms, 5000),
                status="正常",
                record_count=1,
            )
            return str(result.get("trace_id") or result.get("record_id") or "")
        except Exception:
            return ""

    def get_consumer_state(self, consumer_id: int) -> dict[str, Any]:
        """获取消费者状态，包含接续点、可重取偏移和确认明细。"""
        consumer = json_store.get_by_id(self.MODULE, consumer_id)
        if consumer is None:
            return {"ok": False, "message": f"消费者 {consumer_id} 不存在"}

        queue_name = str(consumer.get("队列名称", ""))
        group_name = str(consumer.get("消费组名", ""))
        progress = self._load_progress(consumer)

        return {
            "ok": True,
            "consumer_id": consumer_id,
            "queue": queue_name,
            "group": group_name,
            "status": consumer.get("status"),
            "current_offset": progress["resume"],
            "pending_offsets": sorted(progress["pending"]),
            "confirmed_offsets": len(progress["confirmed"]),
            "offset_confirmations": progress["confirmations"],
            "messages_consumed": consumer.get("messages_consumed", 0),
            "last_commit_at": consumer.get("last_commit_at"),
            "is_healthy": consumer.get("status") == "消费中" and not consumer.get("abnormal"),
        }

    def reset_offset(self, consumer_id: int, target_offset: int) -> dict[str, Any]:
        """重置消费者偏移到指定位置（支持回溯和跳转）。

        重置点之后的确认明细与可重取偏移一并清理，保证回溯后
        接续点与确认明细覆盖范围一致。
        """
        consumer = json_store.get_by_id(self.MODULE, consumer_id)
        if consumer is None:
            return {"ok": False, "message": f"消费者 {consumer_id} 不存在"}

        if consumer.get("status") != "已停止":
            return {"ok": False, "message": "请先停止消费者再重置偏移"}

        queue_name = str(consumer.get("队列名称", ""))
        group_name = str(consumer.get("消费组名", ""))
        offset_key = f"{queue_name}:{group_name}"
        progress = self._load_progress(consumer)
        old_offset = progress["resume"]

        confirmations = [
            c for c in progress["confirmations"] if int(c["offset"]) < target_offset
        ]
        pending = sorted(o for o in progress["pending"] if o < target_offset)
        self._offsets[offset_key] = target_offset

        json_store.update(self.MODULE, consumer_id, {
            "current_offset": target_offset,
            "offset_confirmations": confirmations,
            "pending_offsets": pending,
        })

        return {
            "ok": True,
            "message": f"偏移已从 {old_offset} 重置为 {target_offset}",
            "old_offset": old_offset,
            "new_offset": target_offset,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出消息队列消费的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "消息队列消费数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_messages_consumed": sum(int(r.get("messages_consumed", 0)) for r in rows),
            "active_consumers": sum(1 for r in rows if r.get("status") == "消费中"),
            "operation": self.OPERATION_TERM,
        }
