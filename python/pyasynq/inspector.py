from __future__ import annotations

from pyasynq import _core


class Inspector(_core.Inspector):
    """Queue and task administration, compatible with asynq's Inspector.

    `state` arguments take "pending", "active", "scheduled", "retry",
    "archived", "completed" or "aggregating" (the latter with `group=`).
    """

    async def aget_task_info(self, queue: str, task_id: str) -> _core.TaskInfo:
        return await self._aget_task_info(queue, task_id)

    def list_pending_tasks(self, queue: str, *, page: int = 1, size: int = 30) -> list[_core.TaskInfo]:
        return self.list_tasks(queue, "pending", page=page, size=size)

    def list_active_tasks(self, queue: str, *, page: int = 1, size: int = 30) -> list[_core.TaskInfo]:
        return self.list_tasks(queue, "active", page=page, size=size)

    def list_scheduled_tasks(self, queue: str, *, page: int = 1, size: int = 30) -> list[_core.TaskInfo]:
        return self.list_tasks(queue, "scheduled", page=page, size=size)

    def list_retry_tasks(self, queue: str, *, page: int = 1, size: int = 30) -> list[_core.TaskInfo]:
        return self.list_tasks(queue, "retry", page=page, size=size)

    def list_archived_tasks(self, queue: str, *, page: int = 1, size: int = 30) -> list[_core.TaskInfo]:
        return self.list_tasks(queue, "archived", page=page, size=size)

    def list_completed_tasks(self, queue: str, *, page: int = 1, size: int = 30) -> list[_core.TaskInfo]:
        return self.list_tasks(queue, "completed", page=page, size=size)

    def list_aggregating_tasks(
        self, queue: str, group: str, *, page: int = 1, size: int = 30
    ) -> list[_core.TaskInfo]:
        return self.list_tasks(queue, "aggregating", group=group, page=page, size=size)
