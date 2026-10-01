from __future__ import annotations

from datetime import datetime, timedelta

from src.failure_guard import FailureGuard


def test_failure_guard_opens_circuit_after_threshold_and_rate_limits(tmp_path):
    guard_path = tmp_path / "guard.json"
    cookie_path = tmp_path / "xianyu_state.json"
    cookie_path.write_text("{}", encoding="utf-8")

    guard = FailureGuard(
        path=str(guard_path),
        threshold=3,
        pause_seconds=3 * 24 * 60 * 60,
        tz_name="Asia/Shanghai",
    )

    base = datetime(2026, 3, 4, 12, 0, 0)

    r1 = guard.record_failure("task-a", "err-1", cookie_path=str(cookie_path), now=base)
    assert r1["should_notify"] is False
    assert r1["opened_circuit"] is False

    r2 = guard.record_failure("task-a", "err-2", cookie_path=str(cookie_path), now=base)
    assert r2["should_notify"] is False
    assert r2["opened_circuit"] is False

    r3 = guard.record_failure("task-a", "err-3", cookie_path=str(cookie_path), now=base)
    assert r3["should_notify"] is True
    assert r3["opened_circuit"] is True
    assert r3["paused_until"] is not None

    d0 = guard.should_skip_start("task-a", cookie_path=str(cookie_path), now=base)
    assert d0.skip is True
    assert d0.should_notify is False

    next_day = base + timedelta(days=1, minutes=1)
    d1 = guard.should_skip_start("task-a", cookie_path=str(cookie_path), now=next_day)
    assert d1.skip is True
    assert d1.should_notify is True

    d1b = guard.should_skip_start("task-a", cookie_path=str(cookie_path), now=next_day)
    assert d1b.skip is True
    assert d1b.should_notify is False


def test_failure_guard_auto_recovers_on_cookie_change(tmp_path):
    guard_path = tmp_path / "guard.json"
    cookie_path = tmp_path / "xianyu_state.json"
    cookie_path.write_text("{}", encoding="utf-8")

    guard = FailureGuard(
        path=str(guard_path),
        threshold=2,
        pause_seconds=3 * 24 * 60 * 60,
        tz_name="Asia/Shanghai",
    )

    base = datetime(2026, 3, 4, 12, 0, 0)

    guard.record_failure("task-a", "err-1", cookie_path=str(cookie_path), now=base)
    guard.record_failure("task-a", "err-2", cookie_path=str(cookie_path), now=base)

    paused = guard.should_skip_start("task-a", cookie_path=str(cookie_path), now=base)
    assert paused.skip is True

    cookie_path.write_text('{"updated": true}', encoding="utf-8")

    recovered = guard.should_skip_start(
        "task-a",
        cookie_path=str(cookie_path),
        now=base + timedelta(minutes=1),
    )
    assert recovered.skip is False


def test_failure_guard_persists_state_without_holding_the_data_file_open(tmp_path):
    """回归：Windows 下 os.replace 无法覆盖仍被打开的文件（WinError 5）。

    _update_task 曾经在持有数据文件句柄的同时做原子替换，导致第二次写入
    （此时数据文件已存在）抛 PermissionError，熔断状态永远无法落盘。
    """
    guard_path = tmp_path / "guard.json"
    guard = FailureGuard(
        path=str(guard_path),
        threshold=3,
        pause_seconds=24 * 60 * 60,
        tz_name="Asia/Shanghai",
    )

    base = datetime(2026, 3, 4, 12, 0, 0)

    # 第一次写入创建数据文件，第二次写入必须能原子替换掉已存在的文件。
    guard.record_failure("task-a", "err-1", now=base)
    assert guard_path.exists()

    guard.record_failure("task-a", "err-2", now=base)

    # 状态确实落盘了，而不是只留在 .tmp 里。
    import json

    saved = json.loads(guard_path.read_text(encoding="utf-8"))
    assert saved["tasks"]["task-a"]["consecutive_failures"] == 2
    assert saved["tasks"]["task-a"]["last_failure_reason"] == "err-2"

    # 原子替换已成功，不应留下临时文件。
    assert not (tmp_path / "guard.json.tmp").exists()


def test_failure_guard_locks_a_sidecar_file_not_the_data_file(tmp_path):
    """锁必须放在独立文件上，否则 Windows 的原子替换会失败。"""
    guard_path = tmp_path / "guard.json"
    guard = FailureGuard(path=str(guard_path), tz_name="Asia/Shanghai")

    guard.record_success("task-a")

    assert guard_path.exists()
    assert (tmp_path / "guard.json.lock").exists()
