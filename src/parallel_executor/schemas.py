from pydantic import BaseModel
from typing import Any, Callable, Optional, Generic, TypeVar, Union, Awaitable
from enum import Enum

T = TypeVar('T')


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskInfo(BaseModel):
    """Information about a task in the executor"""
    task_id: str
    name: Optional[str] = None
    status: TaskStatus = TaskStatus.PENDING
    created_at: float
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None


class TaskResult(BaseModel, Generic[T]):
    """Result of a completed task"""
    task_id: str
    result: Optional[T] = None
    error: Optional[str] = None
    completed: bool = False

    @classmethod
    def from_error(cls, task_id: str, error: Exception):
        """Create a TaskResult from an exception"""
        return cls(task_id=task_id, error=str(error), completed=False)


class TaskDefinition(BaseModel, Generic[T]):
    """Definition of a task to be executed"""
    func: Union[Callable[..., T], Callable[..., Awaitable[T]]]  # Support both sync and async functions
    args: tuple = ()
    kwargs: dict = {}
    name: Optional[str] = None
    callback: Optional[Callable[[T], None]] = None