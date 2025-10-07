import asyncio
from typing import List, Optional, Dict, Any, Callable, Awaitable, TypeVar, Union, get_type_hints
from pydantic import BaseModel
from pyee.asyncio import AsyncIOEventEmitter
from datetime import datetime
import time
import uuid

T = TypeVar('T')
R = TypeVar('R')

# Import after type vars are defined
from .schemas import TaskInfo, TaskResult, TaskDefinition, TaskStatus
from .events import TaskEvent


class ParallelExecutor(AsyncIOEventEmitter):
    """
    An asynchronous parallel executor that can run multiple tasks concurrently.
    Supports event emission and callbacks on task completion.
    """
    
    def __init__(self):
        super().__init__()
        self._tasks: Dict[str, asyncio.Task] = {}
        self._task_infos: Dict[str, TaskInfo] = {}
        self._results: Dict[str, TaskResult[Any]] = {}  # Store TaskResult instances of various generic types
        self._task_definitions: Dict[str, Any] = {}  # Store task definitions with their original types
        
    def submit(self, func: Callable[..., Awaitable[T]], *args, callback: Optional[Callable[[T], None]] = None, **kwargs) -> str:
        """
        Submit a task for execution and return its ID.
        
        Args:
            func: The async function to execute
            *args: Arguments to pass to the function
            callback: Optional callback to execute when the task completes
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            str: The task ID
        """
        task_id = str(uuid.uuid4())
        # Create TaskDefinition with proper type binding using generic
        task_definition: TaskDefinition[T] = TaskDefinition(func=func, args=args, kwargs=kwargs, callback=callback)  # type: ignore[arg-type]
        self._task_definitions[task_id] = task_definition
        
        # Create task info
        task_info = TaskInfo(
            task_id=task_id,
            created_at=time.time(),
            name=task_definition.name
        )
        self._task_infos[task_id] = task_info
        
        # Emit task created event
        self.emit(TaskEvent.TASK_CREATED, task_info)
        
        # Schedule the actual execution
        async_task = asyncio.create_task(self._execute_task(task_id, func, *args, **kwargs))
        self._tasks[task_id] = async_task
        
        return task_id
    
    async def _execute_task(self, task_id: str, func: Callable[..., Awaitable[T]], *args, **kwargs) -> None:
        """
        Internal method to execute the task and handle its lifecycle.
        """
        try:
            # Update task status to running
            task_info = self._task_infos[task_id]
            task_info.status = TaskStatus.RUNNING
            task_info.started_at = time.time()
            
            # Emit task started event
            self.emit(TaskEvent.TASK_STARTED, task_info)
            
            # Execute the actual function
            result_value = await func(*args, **kwargs)
            
            # Update task info
            task_info.status = TaskStatus.COMPLETED
            task_info.completed_at = time.time()
            
            # Store the result
            task_result: TaskResult[T] = TaskResult(task_id=task_id, result=result_value, completed=True)
            self._results[task_id] = task_result
            
            # Execute callback if exists
            task_definition: TaskDefinition[T] = self._task_definitions[task_id]
            if task_definition.callback:
                task_definition.callback(result_value)
            
            # Emit task completed event
            self.emit(TaskEvent.TASK_COMPLETED, task_info, task_result)
            
        except Exception as e:
            # Update task info with error
            task_info = self._task_infos[task_id]
            task_info.status = TaskStatus.FAILED
            task_info.completed_at = time.time()
            task_info.error = str(e)
            
            # Store the error
            task_result = TaskResult.from_error(task_id, e)
            self._results[task_id] = task_result
            
            # Emit task failed event
            self.emit(TaskEvent.TASK_FAILED, task_info, task_result)
    
    async def wait_for_all(self, timeout: Optional[float] = None) -> List[TaskResult[Any]]:
        """
        Wait for all submitted tasks to complete.
        
        Args:
            timeout: Optional timeout in seconds
            
        Returns:
            List of task results
        """
        if not self._tasks:
            return []
        
        tasks = list(self._tasks.values())
        await asyncio.wait(tasks, timeout=timeout)
        
        # Return results in the order of task submission
        return [self._results[task_id] for task_id in self._tasks.keys() if task_id in self._results]
    
    async def wait_for_task(self, task_id: str, timeout: Optional[float] = None) -> TaskResult[Any]:
        """
        Wait for a specific task to complete.
        
        Args:
            task_id: The ID of the task to wait for
            timeout: Optional timeout in seconds
            
        Returns:
            The task result
        """
        if task_id not in self._tasks:
            raise ValueError(f"Task with ID {task_id} does not exist")
        
        task = self._tasks[task_id]
        try:
            await asyncio.wait_for(task, timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Task {task_id} timed out")
        
        return self._results[task_id]
    
    def task_info(self, task_id: str) -> Optional[TaskInfo]:
        """
        Get information about a specific task.
        
        Args:
            task_id: The ID of the task to get info for
            
        Returns:
            TaskInfo if the task exists, None otherwise
        """
        return self._task_infos.get(task_id)
    
    @property
    def task_infos(self) -> List[TaskInfo]:
        """
        Get information about all tasks.
        
        Returns:
            List of TaskInfo for all tasks
        """
        return list(self._task_infos.values())
    
    def result(self, task_id: str) -> Any:
        """
        Get the result of a specific task if available.
        
        Args:
            task_id: The ID of the task to get result for
            
        Returns:
            TaskResult if the task has completed, None otherwise
        """
        return self._results.get(task_id)
    
    @property
    def results(self) -> Dict[str, Any]:
        """
        Get all results as a dictionary mapping task_id to result.
        """
        return self._results
    
    async def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a running task.
        
        Args:
            task_id: The ID of the task to cancel
            
        Returns:
            True if the task was cancelled, False if it didn't exist or was already completed
        """
        if task_id not in self._tasks:
            return False
        
        task = self._tasks[task_id]
        if task.done():
            return False  # Already completed
        
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        
        # Update task info
        task_info = self._task_infos[task_id]
        task_info.status = TaskStatus.FAILED
        task_info.completed_at = time.time()
        task_info.error = "Task was cancelled"
        
        # Store the result
        task_result = TaskResult.from_error(task_id, Exception("Task was cancelled"))
        self._results[task_id] = task_result
        
        # Emit task cancelled event
        self.emit(TaskEvent.TASK_CANCELLED, task_info, task_result)
        
        return True
    
    def cancel_all(self) -> None:
        """
        Cancel all running tasks.
        """
        for task_id, task in self._tasks.items():
            if not task.done():
                task.cancel()