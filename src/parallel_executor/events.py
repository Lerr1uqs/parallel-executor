from enum import Enum


class TaskEvent(str, Enum):
    """
    定义任务执行器中所有可能的事件类型
    """
    TASK_CREATED = "task_created"
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_CANCELLED = "task_cancelled"