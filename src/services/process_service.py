"""
进程管理服务
负责管理爬虫进程的启动和停止
"""

import asyncio
import contextlib
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable, Dict, Optional, TextIO

from src.ai_handler import send_ntfy_notification
from src.config import STATE_FILE
from src.failure_guard import FailureGuard
from src.infrastructure.persistence.sqlite_task_repository import find_task_by_name_sync
from src.utils import build_task_log_path

STOP_TIMEOUT_SECONDS = 20
SPIDER_DEBUG_LIMIT_ENV = "SPIDER_DEBUG_LIMIT"
LifecycleHook = Callable[[int], Awaitable[None] | None]


@dataclass(frozen=True)
class StartTaskResult:
    """启动任务的结果。

    启动不了的原因有好几种（已在运行 / 被熔断暂停 / 进程创建失败），原先
    统一返回 False，接口层只能报一句“启动任务失败”，把真正的原因——
    尤其是“任务已暂停到 X”——丢掉了。
    """

    started: bool
    reason: Optional[str] = None


def _describe_abnormal_exit(returncode: Optional[int]) -> Optional[str]:
    """把退出码翻译成可读的异常描述；正常结束返回 None。

    POSIX 下负值表示被信号终止（-9 = SIGKILL，-15 = SIGTERM）；
    Windows 下 terminate() 通常给出正的非零退出码。
    """
    if returncode is None or returncode == 0:
        return None
    if returncode < 0:
        return f"被信号 {-returncode} 终止"
    return f"退出码 {returncode}"


class ProcessService:
    """进程管理服务"""

    def __init__(self):
        self.processes: Dict[int, asyncio.subprocess.Process] = {}
        self.log_paths: Dict[int, str] = {}
        self.log_handles: Dict[int, TextIO] = {}
        self.task_names: Dict[int, str] = {}
        self.exit_watchers: Dict[int, asyncio.Task] = {}
        # 用户主动停止（含后端关闭时的 stop_all）；这类非零退出码不是故障，
        # 不应该发“任务异常退出”告警。
        self._intentional_stops: set[int] = set()
        self.failure_guard = FailureGuard()
        self._on_started: LifecycleHook | None = None
        self._on_stopped: LifecycleHook | None = None

    def set_lifecycle_hooks(
        self,
        *,
        on_started: LifecycleHook | None = None,
        on_stopped: LifecycleHook | None = None,
    ) -> None:
        self._on_started = on_started
        self._on_stopped = on_stopped

    async def _invoke_hook(self, hook: LifecycleHook | None, task_id: int) -> None:
        if hook is None:
            return
        result = hook(task_id)
        if asyncio.iscoroutine(result):
            await result

    def _resolve_cookie_path(self, task_name: str) -> str | None:
        """Best-effort cookie/state path for a task."""
        try:
            task = find_task_by_name_sync(task_name)
            if task and isinstance(task.account_state_file, str) and task.account_state_file.strip():
                return task.account_state_file.strip()
        except Exception:
            pass

        return STATE_FILE if os.path.exists(STATE_FILE) else None

    def is_running(self, task_id: int) -> bool:
        """检查任务是否正在运行"""
        process = self.processes.get(task_id)
        return process is not None and process.returncode is None

    async def _drain_finished_process(self, task_id: int) -> None:
        process = self.processes.get(task_id)
        if process is None or process.returncode is None:
            return

        watcher = self.exit_watchers.get(task_id)
        if watcher is not None:
            await asyncio.shield(watcher)
            return

        self._cleanup_runtime(task_id, process)
        await self._invoke_hook(self._on_stopped, task_id)

    def _open_log_file(self, task_id: int, task_name: str) -> tuple[str, TextIO]:
        os.makedirs("logs", exist_ok=True)
        log_file_path = build_task_log_path(task_id, task_name)
        log_file_handle = open(log_file_path, "a", encoding="utf-8")
        return log_file_path, log_file_handle

    def _build_spawn_command(self, task_name: str) -> list[str]:
        command = [
            sys.executable,
            "-u",
            "spider_v2.py",
            "--task-name",
            task_name,
        ]
        debug_limit = str(os.getenv(SPIDER_DEBUG_LIMIT_ENV, "")).strip()
        if debug_limit.isdigit() and int(debug_limit) > 0:
            command.extend(["--debug-limit", debug_limit])
        return command

    async def _spawn_process(
        self,
        task_name: str,
        log_file_handle: TextIO,
    ) -> asyncio.subprocess.Process:
        preexec_fn = os.setsid if sys.platform != "win32" else None
        child_env = os.environ.copy()
        child_env["PYTHONIOENCODING"] = "utf-8"
        child_env["PYTHONUTF8"] = "1"
        return await asyncio.create_subprocess_exec(
            *self._build_spawn_command(task_name),
            stdout=log_file_handle,
            stderr=log_file_handle,
            preexec_fn=preexec_fn,
            env=child_env,
        )

    def _register_runtime(
        self,
        task_id: int,
        task_name: str,
        process: asyncio.subprocess.Process,
        log_file_path: str,
        log_file_handle: TextIO,
    ) -> None:
        self.processes[task_id] = process
        self.log_paths[task_id] = log_file_path
        self.log_handles[task_id] = log_file_handle
        self.task_names[task_id] = task_name
        self.exit_watchers[task_id] = asyncio.create_task(self._watch_process_exit(process))

    async def start_task(self, task_id: int, task_name: str) -> StartTaskResult:
        """启动任务进程。"""
        await self._drain_finished_process(task_id)
        if self.is_running(task_id):
            print(f"任务 '{task_name}' (ID: {task_id}) 已在运行中")
            return StartTaskResult(False, "任务已在运行中")

        decision = self.failure_guard.should_skip_start(
            task_name,
            cookie_path=self._resolve_cookie_path(task_name),
        )
        if decision.skip:
            await self._notify_skip(task_name, decision)
            return StartTaskResult(False, self._format_skip_reason(decision))

        log_file_path = ""
        log_file_handle = None
        try:
            log_file_path, log_file_handle = self._open_log_file(task_id, task_name)
            process = await self._spawn_process(task_name, log_file_handle)
        except Exception as exc:
            self._close_log_handle(log_file_handle)
            print(f"启动任务 '{task_name}' 失败: {exc}")
            return StartTaskResult(False, f"创建任务进程失败: {exc}")

        self._register_runtime(task_id, task_name, process, log_file_path, log_file_handle)
        print(f"启动任务 '{task_name}' (PID: {process.pid})")
        await self._invoke_hook(self._on_started, task_id)
        return StartTaskResult(True)

    def _format_skip_reason(self, decision) -> str:
        """把熔断跳过决策转成用户能直接读懂的一句话。"""
        until_text = (
            decision.paused_until.strftime("%Y-%m-%d %H:%M:%S")
            if decision.paused_until
            else "N/A"
        )
        return (
            f"任务处于暂停状态（连续失败 {decision.consecutive_failures}/"
            f"{self.failure_guard.threshold}），暂停到 {until_text}。"
            f"原因: {decision.reason}。"
            "更新登录态/cookies 文件后会自动恢复。"
        )

    async def _notify_skip(self, task_name: str, decision) -> None:
        print(
            f"[FailureGuard] 跳过启动任务 '{task_name}'，已暂停重试 "
            f"(连续失败 {decision.consecutive_failures}/{self.failure_guard.threshold})"
        )
        if not decision.should_notify:
            return
        try:
            await send_ntfy_notification(
                {
                    "商品标题": f"[任务暂停] {task_name}",
                    "当前售价": "N/A",
                    "商品链接": "#",
                },
                "任务处于暂停状态，将跳过执行。\n"
                f"原因: {decision.reason}\n"
                f"连续失败: {decision.consecutive_failures}/{self.failure_guard.threshold}\n"
                f"暂停到: {decision.paused_until.strftime('%Y-%m-%d %H:%M:%S') if decision.paused_until else 'N/A'}\n"
                "修复方法: 更新登录态/cookies文件后会自动恢复。",
            )
        except Exception as exc:
            print(f"发送任务暂停通知失败: {exc}")

    async def _watch_process_exit(self, process: asyncio.subprocess.Process) -> None:
        await process.wait()
        task_id = self._find_task_id_by_process(process)
        if task_id is None:
            return

        # 注意：_cleanup_runtime 会清掉这些索引，所以先取出来。
        task_name = self.task_names.get(task_id, f"任务 {task_id}")
        log_path = self.log_paths.get(task_id)
        stopped_intentionally = task_id in self._intentional_stops
        self._intentional_stops.discard(task_id)

        self._cleanup_runtime(task_id, process)
        await self._invoke_hook(self._on_stopped, task_id)

        # 进程猝死（浏览器/驱动被 OOM 杀掉、EPIPE 等）必须让用户知道。
        # 原先完全忽略 returncode，于是“崩了”和“跑完了”在界面上长得一样，
        # 无人值守时一个已经崩掉的任务会一直看着像正常运行。
        if stopped_intentionally:
            return
        detail = _describe_abnormal_exit(process.returncode)
        if detail is None:
            return
        self._append_log_marker(
            log_path, f"!!! 任务进程异常退出（{detail}），本次结果可能不完整 !!!"
        )
        await self._notify_abnormal_exit(task_name, detail, process.returncode, log_path)

    async def _notify_abnormal_exit(
        self,
        task_name: str,
        detail: str,
        returncode: Optional[int],
        log_path: Optional[str],
    ) -> None:
        print(f"[ProcessService] 任务 '{task_name}' 异常退出（{detail}）")
        try:
            await send_ntfy_notification(
                {
                    "商品标题": f"[任务异常退出] {task_name}",
                    "当前售价": "N/A",
                    "商品链接": "#",
                },
                "任务进程异常退出，本次运行结果可能不完整。\n"
                f"任务: {task_name}\n"
                f"原因: {detail}（退出码 {returncode}）\n"
                f"日志: {log_path or 'N/A'}\n"
                "常见原因: 浏览器/驱动因内存不足被系统终止，或进程被外部杀掉。\n"
                "排查: 确认系统可用内存，必要时降低抓取并发。",
            )
        except Exception as exc:
            print(f"发送任务异常退出通知失败: {exc}")

    def _find_task_id_by_process(self, process: asyncio.subprocess.Process) -> int | None:
        for task_id, current_process in self.processes.items():
            if current_process is process:
                return task_id
        return None

    def _cleanup_runtime(
        self,
        task_id: int,
        process: asyncio.subprocess.Process,
    ) -> None:
        if self.processes.get(task_id) is not process:
            return
        self.processes.pop(task_id, None)
        self.log_paths.pop(task_id, None)
        self.task_names.pop(task_id, None)
        self._close_log_handle(self.log_handles.pop(task_id, None))
        self.exit_watchers.pop(task_id, None)

    def _close_log_handle(self, log_handle: TextIO | None) -> None:
        if log_handle is None:
            return
        with contextlib.suppress(Exception):
            log_handle.close()

    def _append_log_marker(self, log_path: Optional[str], message: str) -> None:
        if not log_path:
            return
        try:
            timestamp = datetime.now().strftime(" %Y-%m-%d %H:%M:%S")
            with open(log_path, "a", encoding="utf-8") as log_file:
                log_file.write(f"[{timestamp}] {message}\n")
        except Exception as exc:
            print(f"写入任务日志标记失败: {exc}")

    def _append_stop_marker(self, log_path: str | None) -> None:
        self._append_log_marker(log_path, "!!! 任务已被终止 !!!")

    async def stop_task(self, task_id: int) -> bool:
        """停止任务进程"""
        await self._drain_finished_process(task_id)
        process = self.processes.get(task_id)
        if process is None:
            print(f"任务 ID {task_id} 没有正在运行的进程")
            return False
        if process.returncode is not None:
            await self._await_exit_watcher(task_id)
            print(f"任务进程 {process.pid} (ID: {task_id}) 已退出，略过停止")
            return False

        try:
            # 必须在终止之前登记，否则退出监听可能在还未来得及标记时就跑完了。
            self._intentional_stops.add(task_id)
            await self._terminate_process(process, task_id)
            self._append_stop_marker(self.log_paths.get(task_id))
            await self._await_exit_watcher(task_id)
            print(f"任务进程 {process.pid} (ID: {task_id}) 已终止")
            return True
        except ProcessLookupError:
            self._intentional_stops.discard(task_id)
            print(f"进程 (ID: {task_id}) 已不存在")
            return False
        except Exception as exc:
            self._intentional_stops.discard(task_id)
            print(f"停止任务进程 (ID: {task_id}) 时出错: {exc}")
            return False

    async def _terminate_process(
        self,
        process: asyncio.subprocess.Process,
        task_id: int,
    ) -> None:
        if sys.platform != "win32":
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        else:
            process.terminate()

        try:
            await asyncio.wait_for(process.wait(), timeout=STOP_TIMEOUT_SECONDS)
            return
        except asyncio.TimeoutError:
            print(
                f"任务进程 {process.pid} (ID: {task_id}) 未在 "
                f"{STOP_TIMEOUT_SECONDS} 秒内退出，准备强制终止..."
            )

        if sys.platform != "win32":
            with contextlib.suppress(ProcessLookupError):
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        else:
            process.kill()
        await process.wait()

    async def _await_exit_watcher(self, task_id: int) -> None:
        watcher = self.exit_watchers.get(task_id)
        if watcher is None:
            return
        await asyncio.shield(watcher)

    def reindex_after_delete(self, deleted_task_id: int) -> None:
        """删除任务后同步重排运行时索引，避免任务下标漂移。"""
        self.processes = self._reindex_mapping(self.processes, deleted_task_id)
        self.log_paths = self._reindex_mapping(self.log_paths, deleted_task_id)
        self.log_handles = self._reindex_mapping(self.log_handles, deleted_task_id)
        self.task_names = self._reindex_mapping(self.task_names, deleted_task_id)
        self.exit_watchers = self._reindex_mapping(self.exit_watchers, deleted_task_id)
        # 主动停止标记只存活到进程退出（秒级），删除任务时直接丢弃，
        # 避免残留标记误压制另一个任务的“异常退出”告警。
        self._intentional_stops.discard(deleted_task_id)

    def _reindex_mapping(self, mapping: Dict[int, object], deleted_task_id: int) -> Dict[int, object]:
        reindexed: Dict[int, object] = {}
        for task_id, value in mapping.items():
            if task_id == deleted_task_id:
                continue
            next_task_id = task_id - 1 if task_id > deleted_task_id else task_id
            reindexed[next_task_id] = value
        return reindexed

    async def stop_all(self) -> None:
        """停止所有任务进程"""
        task_ids = list(self.processes.keys())
        for task_id in task_ids:
            await self.stop_task(task_id)
