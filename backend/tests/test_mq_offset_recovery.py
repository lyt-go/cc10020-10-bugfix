"""消息消费偏移确认与死信缺口的回归测试。

验收场景：同一批次两条消息，一条处理失败且死信写入也失败（未入死信），
后续偏移那条已处理成功。重启消费组后：
- 死信未登记的原消息数据仍能从原偏移重新读取（进入补做范围）；
- 后续已确认的偏移保持原消费结果，不重复确认；
- 消费失败计数与死信处置列表对齐，确认明细反映真实处置结果。
"""
from __future__ import annotations

import os
import random
import sys
import tempfile
import threading

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app.persistence
from app.persistence import JsonStore
from app.services.mq_consumer import MqConsumerService
from app.services.dlq_handler import DlqHandlerService


@pytest.fixture(autouse=True)
def _isolated_store(monkeypatch):
    """每个测试切换到独立临时仓库，monkeypatch 自动还原，避免跨文件污染。"""
    with tempfile.TemporaryDirectory() as tmp:
        store = JsonStore(base_dir=tmp)
        monkeypatch.setattr(app.persistence, "json_store", store)
        import app.services.mq_consumer as mq_mod
        import app.services.dlq_handler as dlq_mod
        import app.services.call_trace_log as ctl_mod
        monkeypatch.setattr(mq_mod, "json_store", store)
        monkeypatch.setattr(dlq_mod, "json_store", store)
        monkeypatch.setattr(ctl_mod, "json_store", store)
        yield store


@pytest.fixture()
def json_store(_isolated_store):
    return _isolated_store


def _create_running_consumer(svc: MqConsumerService, json_store, batch_size: str = "2") -> int:
    """登记消费配置并启动消费，返回消费者ID。"""
    entry, missing = svc.create_entry({
        "配置编号": "MQ-REC-001",
        "队列名称": "recovery_queue",
        "消费组名": "recovery_group",
        "偏移策略": "earliest",
        "批量大小": batch_size,
        "超时时间": "5000",
        "死信队列": "recovery_dlq",
        "消费状态": "消费中",
    })
    assert not missing
    consumer_id = entry["id"]
    svc.run_action(consumer_id, "启动消费")
    assert json_store.get_by_id("mq_consumer", consumer_id)["status"] == "消费中"
    return consumer_id


# ──────────────────────────────────────────────────────────
# 验收场景：失败且未入死信的原消息不丢失，重启后可补做
# ──────────────────────────────────────────────────────────
def test_failed_message_without_dlq_registration_is_recoverable(monkeypatch, json_store):
    svc = MqConsumerService()
    consumer_id = _create_running_consumer(svc, json_store)

    # 同一批次两条消息：offset 0 处理失败且死信写入失败，offset 1 处理成功
    draws = iter([0.1, 0.9])
    monkeypatch.setattr(random, "random", lambda: next(draws))

    original_enqueue = DlqHandlerService.enqueue_dead_letter

    def _dlq_unavailable(*args, **kwargs):
        raise RuntimeError("dlq store unavailable")

    monkeypatch.setattr(DlqHandlerService, "enqueue_dead_letter", _dlq_unavailable)

    result = svc.consume_batch(consumer_id, max_messages=2)

    # 消费失败计数与死信处置列表对齐：失败 1 条但未登记，死信清单为空
    assert result["ok"]
    assert result["consumed"] == 1
    assert result["failed"] == 1
    assert result["dlq_registered"] == 0
    assert result["unresolved"] == 1
    assert json_store.get_all("dlq_handler") == []

    # 有效消费进度：接续点停在未处置偏移，可重取偏移保留原消息位置
    assert result["previous_offset"] == 0
    assert result["new_offset"] == 0
    assert result["pending_offsets"] == [0]

    consumer = json_store.get_by_id("mq_consumer", consumer_id)
    # 确认明细只反映真实处置结果：offset 1 已确认，offset 0 未确认
    assert [c["offset"] for c in consumer["offset_confirmations"]] == [1]
    assert consumer["offset_confirmations"][0]["result"] == "consumed"
    assert consumer["pending_offsets"] == [0]
    assert consumer["messages_consumed"] == 1

    # ── 重启消费组：新实例内存偏移丢失，从持久化进度恢复 ──
    svc_restarted = MqConsumerService()
    monkeypatch.setattr(DlqHandlerService, "enqueue_dead_letter", original_enqueue)  # 死信写入恢复
    monkeypatch.setattr(random, "random", lambda: 0.9)  # 本次全部处理成功

    recovered = svc_restarted.consume_batch(consumer_id, max_messages=2)

    # 死信未登记的原消息仍能从原偏移重新读取，进入补做范围
    assert recovered["previous_offset"] == 0
    assert recovered["offsets"][0] == 0
    re_read = recovered["messages"][0]
    assert re_read["offset"] == 0
    assert re_read["payload"]["data"]["id"] == 1
    assert re_read["payload"]["data"]["value"] == "record_1"

    # 补做成功：offset 0 进入确认明细，接续点越过已确认区间
    assert recovered["consumed"] == 2  # offset 0 补做 + 新偏移 2
    assert recovered["new_offset"] == 3
    assert recovered["pending_offsets"] == []

    consumer = json_store.get_by_id("mq_consumer", consumer_id)
    confirmed_offsets = sorted(c["offset"] for c in consumer["offset_confirmations"])
    assert confirmed_offsets == [0, 1, 2]
    # 后续已确认的偏移保持原消费结果：offset 1 只确认一次，不重复计数
    assert len([c for c in consumer["offset_confirmations"] if c["offset"] == 1]) == 1
    assert consumer["messages_consumed"] == 3


# ──────────────────────────────────────────────────────────
# 死信登记凭证关联确认明细，登记结果回写消费清单
# ──────────────────────────────────────────────────────────
def test_dlq_registration_credential_reconciles_with_confirmation(monkeypatch, json_store):
    svc = MqConsumerService()
    consumer_id = _create_running_consumer(svc, json_store)

    # 两条消息都处理失败，死信写入正常
    monkeypatch.setattr(random, "random", lambda: 0.1)
    result = svc.consume_batch(consumer_id, max_messages=2)

    assert result["consumed"] == 0
    assert result["failed"] == 2
    assert result["dlq_registered"] == 2
    assert result["unresolved"] == 0

    # 消费失败计数与死信处置列表一致
    dlq_rows = json_store.get_all("dlq_handler")
    assert len(dlq_rows) == result["dlq_registered"] == 2

    # 死信记录关联来源消费偏移与消费者
    assert sorted(r["_offset"] for r in dlq_rows) == [0, 1]
    assert all(r["_consumer_id"] == consumer_id for r in dlq_rows)

    # 登记凭证（记录编号 + 死信ID）回写消费偏移确认明细
    consumer = json_store.get_by_id("mq_consumer", consumer_id)
    confirmations = sorted(consumer["offset_confirmations"], key=lambda c: c["offset"])
    assert [c["result"] for c in confirmations] == ["dlq_registered", "dlq_registered"]
    by_id = {r["id"]: r for r in dlq_rows}
    for conf, row in zip(confirmations, sorted(dlq_rows, key=lambda r: r["_offset"])):
        assert conf["dlq_id"] == row["id"]
        assert conf["credential"] == row["记录编号"] == by_id[conf["dlq_id"]]["记录编号"]

    # 两条失败消息均已处置：无可重取偏移，接续点越过本批次
    assert consumer["pending_offsets"] == []
    assert consumer["current_offset"] == 2
    assert consumer["messages_consumed"] == 2


# ──────────────────────────────────────────────────────────
# 并发续做：已完成的原偏移只确认一次
# ──────────────────────────────────────────────────────────
def test_concurrent_resume_confirms_offset_once(monkeypatch, json_store):
    svc = MqConsumerService()
    consumer_id = _create_running_consumer(svc, json_store)

    monkeypatch.setattr(random, "random", lambda: 0.9)  # 全部处理成功

    # 用屏障强制两个消费者实例在同一批次内并发续做同一批偏移
    barrier = threading.Barrier(2)
    original_record = MqConsumerService._record_consumption

    def _synced_record(self, cid, msg, latency_ms):
        barrier.wait(timeout=10)
        return original_record(self, cid, msg, latency_ms)

    monkeypatch.setattr(MqConsumerService, "_record_consumption", _synced_record)

    results = []

    def _consume():
        results.append(MqConsumerService().consume_batch(consumer_id, max_messages=2))

    threads = [threading.Thread(target=_consume) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert all(r["ok"] for r in results)
    # 两个并发续做者中，同一原偏移只被确认一次
    assert sum(r["consumed"] for r in results) == 2
    assert sum(r["skipped_confirmed"] for r in results) == 2

    consumer = json_store.get_by_id("mq_consumer", consumer_id)
    offsets = sorted(c["offset"] for c in consumer["offset_confirmations"])
    assert offsets == [0, 1]
    assert consumer["messages_consumed"] == 2


def test_apply_dispositions_is_idempotent_for_same_offset(json_store):
    """同一原偏移重复提交确认（并发续做交错）只生效一次。"""
    svc = MqConsumerService()
    consumer_id = _create_running_consumer(svc, json_store)
    offset_key = "recovery_queue:recovery_group"

    disposition = [{"offset": 0, "result": "consumed", "credential": "trace-1"}]
    first = svc._apply_dispositions(consumer_id, offset_key, disposition)
    second = MqConsumerService()._apply_dispositions(consumer_id, offset_key, list(disposition))

    assert first["consumed"] == 1
    assert second["consumed"] == 0
    assert second["skipped_confirmed"] == 1

    consumer = json_store.get_by_id("mq_consumer", consumer_id)
    assert [c["offset"] for c in consumer["offset_confirmations"]] == [0]
    assert consumer["messages_consumed"] == 1


# ──────────────────────────────────────────────────────────
# 凭证校验：历史确认明细中死信记录缺失的偏移恢复时可重新定位
# ──────────────────────────────────────────────────────────
def test_credential_validation_recovers_missing_dlq_record(monkeypatch, json_store):
    svc = MqConsumerService()
    consumer_id = _create_running_consumer(svc, json_store)

    # 构造历史偏移记录：offset 4 已消费，offset 5 登记死信但死信记录已缺失
    json_store.update("mq_consumer", consumer_id, {
        "current_offset": 6,
        "messages_consumed": 2,
        "offset_confirmations": [
            {"offset": 4, "result": "consumed", "credential": "trace-4",
             "dlq_id": None, "confirmed_at": "2026-11-20T00:00:00+00:00"},
            {"offset": 5, "result": "dlq_registered", "credential": "DLQ-LOST",
             "dlq_id": 999, "confirmed_at": "2026-11-20T00:00:00+00:00"},
        ],
    })

    monkeypatch.setattr(random, "random", lambda: 0.9)
    result = svc.consume_batch(consumer_id, max_messages=2)

    # offset 5 凭证校验失败 → 重新定位丢失消息，从原偏移补拉
    assert result["previous_offset"] == 5
    assert result["offsets"] == [5, 6]

    consumer = json_store.get_by_id("mq_consumer", consumer_id)
    confirmed = {c["offset"]: c["result"] for c in consumer["offset_confirmations"]}
    assert confirmed == {4: "consumed", 5: "consumed", 6: "consumed"}
    assert consumer["pending_offsets"] == []
    assert consumer["current_offset"] == 7


# ──────────────────────────────────────────────────────────
# 兼容历史偏移记录：无确认明细字段的旧记录按 current_offset 续作
# ──────────────────────────────────────────────────────────
def test_legacy_consumer_record_without_ledger_still_consumes(monkeypatch, json_store):
    legacy = json_store.insert("mq_consumer", {
        "配置编号": "MQ-LEGACY",
        "队列名称": "legacy_queue",
        "消费组名": "legacy_group",
        "批量大小": "3",
        "status": "消费中",
        "current_offset": 10,
        "messages_consumed": 10,
    })

    monkeypatch.setattr(random, "random", lambda: 0.9)
    svc = MqConsumerService()
    result = svc.consume_batch(legacy["id"], max_messages=3)

    assert result["ok"]
    assert result["previous_offset"] == 10
    assert result["offsets"] == [10, 11, 12]
    assert result["new_offset"] == 13

    consumer = json_store.get_by_id("mq_consumer", legacy["id"])
    assert consumer["messages_consumed"] == 13
    assert sorted(c["offset"] for c in consumer["offset_confirmations"]) == [10, 11, 12]
