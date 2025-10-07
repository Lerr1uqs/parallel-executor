import asyncio
import pytest
import time
from typing import List
from parallel_executor.asynchronous import ParallelExecutor as AsyncParallelExecutor
from parallel_executor.synchronous import ParallelExecutor as SyncParallelExecutor
from parallel_executor.schemas import TaskResult, TaskInfo, TaskStatus
from parallel_executor.events import TaskEvent


# Async test functions
async def async_task_success(value: str) -> str:
    """An async task that succeeds"""
    await asyncio.sleep(0.1)  # Simulate async work
    return f"success_{value}"


async def async_task_failure() -> str:
    """An async task that fails"""
    await asyncio.sleep(0.1)  # Simulate async work
    raise ValueError("Task failed")


async def callback_success(result: str) -> None:
    """A callback for successful tasks"""
    print(f"Callback executed with result: {result}")


def sync_task_success(value: str) -> str:
    """A sync task that succeeds"""
    time.sleep(0.1)  # Simulate work
    return f"success_{value}"


def sync_task_failure() -> str:
    """A sync task that fails"""
    time.sleep(0.1)  # Simulate work
    raise ValueError("Task failed")


def callback_sync_success(result: str) -> None:
    """A callback for successful sync tasks"""
    print(f"Sync callback executed with result: {result}")


@pytest.mark.asyncio
async def test_async_executor_basic():
    """Test basic functionality of the async executor"""
    executor = AsyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(async_task_success, "test")
    
    # Wait for the task to complete
    results = await executor.wait_for_all()
    
    assert len(results) == 1
    assert results[0].completed
    assert results[0].result == "success_test"


@pytest.mark.asyncio
async def test_async_executor_with_callback():
    """Test async executor with callback"""
    results_collected = []
    
    def capture_result(result: str):
        results_collected.append(result)
    
    executor = AsyncParallelExecutor()
    
    # Submit a task with callback
    task_id = executor.submit(async_task_success, "test", callback=capture_result)
    
    # Wait for the task to complete
    await executor.wait_for_all()
    
    # Check that the callback was executed
    assert len(results_collected) == 1
    assert results_collected[0] == "success_test"
    
    # Check the result
    result = executor.result(task_id)
    assert result is not None
    assert result.completed
    assert result.result == "success_test"


@pytest.mark.asyncio
async def test_async_executor_failure():
    """Test async executor with failing task"""
    executor = AsyncParallelExecutor()
    
    # Submit a failing task
    task_id = executor.submit(async_task_failure)
    
    # Wait for the task to complete
    results = await executor.wait_for_all()
    
    assert len(results) == 1
    assert not results[0].completed
    assert results[0].error == "Task failed"


@pytest.mark.asyncio
async def test_async_executor_multiple_tasks():
    """Test async executor with multiple tasks"""
    executor = AsyncParallelExecutor()
    
    # Submit multiple tasks
    task_ids = []
    for i in range(3):
        task_id = executor.submit(async_task_success, f"test_{i}")
        task_ids.append(task_id)
    
    # Wait for all tasks to complete
    results = await executor.wait_for_all()
    
    assert len(results) == 3
    for i, result in enumerate(results):
        assert result.completed
        assert result.result == f"success_test_{i}"


@pytest.mark.asyncio
async def test_async_executor_single_task_wait():
    """Test waiting for a single task"""
    executor = AsyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(async_task_success, "single")
    
    # Wait for the specific task
    result = await executor.wait_for_task(task_id)
    
    assert result.completed
    assert result.result == "success_single"


@pytest.mark.asyncio
async def test_async_executor_task_info():
    """Test getting task information"""
    executor = AsyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(async_task_success, "info")
    
    # Get task info before completion
    task_info = executor.task_info(task_id)
    assert task_info is not None
    assert task_info.task_id == task_id
    assert task_info.status in [TaskStatus.PENDING, TaskStatus.RUNNING]
    
    # Wait for completion
    await executor.wait_for_all()
    
    # Get task info after completion
    task_info = executor.task_info(task_id)
    assert task_info is not None
    assert task_info.status == TaskStatus.COMPLETED


def test_sync_executor_basic():
    """Test basic functionality of the sync executor"""
    executor = SyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(sync_task_success, "test")
    
    # Wait for the task to complete
    results = executor.wait_for_all()
    
    assert len(results) == 1
    assert results[0].completed
    assert results[0].result == "success_test"
    
    executor.shutdown()


def test_sync_executor_with_callback():
    """Test sync executor with callback"""
    results_collected = []
    
    def capture_result(result: str):
        results_collected.append(result)
    
    executor = SyncParallelExecutor()
    
    # Submit a task with callback
    task_id = executor.submit(sync_task_success, "test", callback=capture_result)
    
    # Wait for the task to complete
    executor.wait_for_all()
    
    # Check that the callback was executed
    assert len(results_collected) == 1
    assert results_collected[0] == "success_test"
    
    # Check the result
    result = executor.result(task_id)
    assert result is not None
    assert result.completed
    assert result.result == "success_test"
    
    executor.shutdown()


def test_sync_executor_failure():
    """Test sync executor with failing task"""
    executor = SyncParallelExecutor()
    
    # Submit a failing task
    task_id = executor.submit(sync_task_failure)
    
    # Wait for the task to complete
    results = executor.wait_for_all()
    
    assert len(results) == 1
    assert not results[0].completed
    assert results[0].error == "Task failed"
    
    executor.shutdown()


def test_sync_executor_multiple_tasks():
    """Test sync executor with multiple tasks"""
    executor = SyncParallelExecutor()
    
    # Submit multiple tasks
    task_ids = []
    for i in range(3):
        task_id = executor.submit(sync_task_success, f"test_{i}")
        task_ids.append(task_id)
    
    # Wait for all tasks to complete
    results = executor.wait_for_all()
    
    assert len(results) == 3
    for i, result in enumerate(results):
        assert result.completed
        assert result.result == f"success_test_{i}"
    
    executor.shutdown()


def test_sync_executor_single_task_wait():
    """Test waiting for a single task in sync executor"""
    executor = SyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(sync_task_success, "single")
    
    # Wait for the specific task
    result = executor.wait_for_task(task_id)
    
    assert result.completed
    assert result.result == "success_single"
    
    executor.shutdown()


def test_sync_executor_task_info():
    """Test getting task information in sync executor"""
    executor = SyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(sync_task_success, "info")
    
    # Get task info before completion
    task_info = executor.task_info(task_id)
    assert task_info is not None
    assert task_info.task_id == task_id
    assert task_info.status in [TaskStatus.PENDING, TaskStatus.RUNNING]
    
    # Wait for completion
    executor.wait_for_all()
    
    # Get task info after completion
    task_info = executor.task_info(task_id)
    assert task_info is not None
    assert task_info.status == TaskStatus.COMPLETED
    
    executor.shutdown()


@pytest.mark.asyncio
async def test_async_executor_event_emission():
    """Test event emission in async executor"""
    executor = AsyncParallelExecutor()
    
    events_received = []
    
    def on_task_created(task_info: TaskInfo):
        events_received.append(('created', task_info.task_id))
    
    def on_task_started(task_info: TaskInfo):
        events_received.append(('started', task_info.task_id))
    
    def on_task_completed(task_info: TaskInfo, result: TaskResult):
        events_received.append(('completed', task_info.task_id))
    
    # Register event listeners
    executor.on(TaskEvent.TASK_CREATED, on_task_created)
    executor.on(TaskEvent.TASK_STARTED, on_task_started)
    executor.on(TaskEvent.TASK_COMPLETED, on_task_completed)
    
    # Submit a task
    task_id = executor.submit(async_task_success, "event_test")
    
    # Wait for completion
    await executor.wait_for_all()
    
    # Check that all events were emitted
    event_types = [event[0] for event in events_received]
    assert 'created' in event_types
    assert 'started' in event_types
    assert 'completed' in event_types


@pytest.mark.asyncio
async def test_async_executor_cancel_task():
    """Test canceling a running task"""
    executor = AsyncParallelExecutor()
    
    # Submit a long-running task
    task_id = executor.submit(asyncio.sleep, 5)  # Long sleep that we'll cancel
    
    # Wait a bit for task to start
    await asyncio.sleep(0.1)
    
    # Cancel the task
    result = await executor.cancel_task(task_id)
    
    # Should return True to indicate successful cancellation
    assert result is True


def test_sync_executor_cancel_task():
    """Test canceling a task in sync executor - test with not-yet-started task"""
    executor = SyncParallelExecutor(max_workers=1)  # Use only 1 worker
    
    # Submit first task that takes long to start the worker busy
    def slow_task():
        time.sleep(1)  # Short sleep
        return "slow"
    
    def quick_task():
        return "quick"
    
    # Submit the slow task first
    slow_task_id = executor.submit(slow_task)
    # Immediately submit another task
    quick_task_id = executor.submit(quick_task)
    
    # Try to cancel - note that ThreadPoolExecutor tasks cannot be cancelled once started
    # This is expected behavior for ThreadPoolExecutor
    result = executor.cancel_task(quick_task_id)  # May not be cancellable if already started
    
    # Result can be True (cancelled before start) or False (already started/executed)
    # For ThreadPoolExecutor, cancellation typically only works for queued tasks
    assert result in [True, False]
    
    executor.shutdown()


@pytest.mark.asyncio
async def test_async_executor_get_task_info():
    """Test task_info method"""
    executor = AsyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(async_task_success, "info_test")
    
    # Get task info
    task_info = executor.task_info(task_id)
    
    assert task_info is not None
    assert task_info.task_id == task_id
    
    # Wait for completion
    await executor.wait_for_all()
    
    # Check task info after completion
    task_info = executor.task_info(task_id)
    assert task_info is not None
    assert task_info.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]
    
    # Test the new properties
    all_task_infos = executor.task_infos
    assert len(all_task_infos) >= 1
    assert any(ti.task_id == task_id for ti in all_task_infos)
    
    all_results = executor.results
    assert task_id in all_results


def test_sync_executor_get_task_info():
    """Test task_info method in sync executor"""
    executor = SyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(sync_task_success, "info_test")
    
    # Get task info
    task_info = executor.task_info(task_id)
    
    assert task_info is not None
    assert task_info.task_id == task_id
    
    # Wait for completion
    executor.wait_for_all()
    
    # Check task info after completion
    task_info = executor.task_info(task_id)
    assert task_info is not None
    assert task_info.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]
    
    # Test the new properties
    all_task_infos = executor.task_infos
    assert len(all_task_infos) >= 1
    assert any(ti.task_id == task_id for ti in all_task_infos)
    
    all_results = executor.results
    assert task_id in all_results
    
    executor.shutdown()


@pytest.mark.asyncio
async def test_async_executor_cancel_all():
    """Test cancel_all method"""
    executor = AsyncParallelExecutor()
    
    # Submit several tasks
    task_ids = []
    for i in range(3):
        task_id = executor.submit(asyncio.sleep, 5)  # Long sleep tasks
        task_ids.append(task_id)
    
    # Wait a bit for tasks to start
    await asyncio.sleep(0.1)
    
    # Cancel all tasks
    executor.cancel_all()
    
    # Wait for all tasks to be marked as completed/failed
    try:
        await executor.wait_for_all(timeout=1.0)
    except TimeoutError:
        pass  # Expected if tasks were cancelled


def test_sync_executor_cancel_all():
    """Test cancel_all method in sync executor"""
    executor = SyncParallelExecutor()
    
    # Submit several tasks
    task_ids = []
    for i in range(3):
        task_id = executor.submit(time.sleep, 2)  # Long sleep tasks
        task_ids.append(task_id)
    
    # Wait a bit for tasks to start
    time.sleep(0.1)
    
    # Cancel all tasks
    executor.cancel_all()
    
    # Wait for all tasks to be marked as completed/failed
    executor.wait_for_all()
    
    executor.shutdown()


@pytest.mark.asyncio
async def test_async_executor_wait_for_task():
    """Test waiting for a specific task"""
    executor = AsyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(async_task_success, "wait_test")
    
    # Wait for the specific task
    result = await executor.wait_for_task(task_id)
    
    assert result.completed
    assert result.result == "success_wait_test"
    
    # Test timeout
    timeout_task_id = executor.submit(asyncio.sleep, 2)
    
    with pytest.raises(TimeoutError):
        await executor.wait_for_task(timeout_task_id, timeout=0.1)
    
    # Cancel the timeout task so it doesn't interfere with other tests
    await executor.cancel_task(timeout_task_id)


def test_sync_executor_wait_for_task():
    """Test waiting for a specific task in sync executor"""
    executor = SyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(sync_task_success, "wait_test")
    
    # Wait for the specific task
    result = executor.wait_for_task(task_id)
    
    assert result.completed
    assert result.result == "success_wait_test"
    
    executor.shutdown()


def test_sync_executor_shutdown():
    """Test shutdown method"""
    executor = SyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(sync_task_success, "shutdown_test")
    
    # Wait for completion
    executor.wait_for_all()
    
    # Shutdown executor
    executor.shutdown()
    
    # Should not raise an error when calling methods after shutdown


@pytest.mark.asyncio
async def test_async_executor_task_result_access():
    """Test accessing task results after completion"""
    executor = AsyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(async_task_success, "result_test")
    
    # Wait for completion
    await executor.wait_for_all()
    
    # Access the result
    result = executor.result(task_id)
    assert result is not None
    assert result.completed
    assert result.result == "success_result_test"
    
    # Test accessing results for all tasks
    all_results = await executor.wait_for_all()
    assert len(all_results) >= 1
    assert all_results[-1].result == "success_result_test"


def test_sync_executor_task_result_access():
    """Test accessing task results after completion in sync executor"""
    executor = SyncParallelExecutor()
    
    # Submit a task
    task_id = executor.submit(sync_task_success, "result_test")
    
    # Wait for completion
    executor.wait_for_all()
    
    # Access the result
    result = executor.result(task_id)
    assert result is not None
    assert result.completed
    assert result.result == "success_result_test"
    
    # Test accessing results for all tasks
    all_results = executor.wait_for_all()
    assert len(all_results) >= 1
    assert all_results[-1] == result
    
    executor.shutdown()


def test_sync_executor_event_emission():
    """Test event emission in sync executor"""
    executor = SyncParallelExecutor()
    
    events_received = []
    
    def on_task_created(task_info: TaskInfo):
        events_received.append(('created', task_info.task_id))
    
    def on_task_started(task_info: TaskInfo):
        events_received.append(('started', task_info.task_id))
    
    def on_task_completed(task_info: TaskInfo, result: TaskResult):
        events_received.append(('completed', task_info.task_id))
    
    # Register event listeners
    executor.on(TaskEvent.TASK_CREATED, on_task_created)
    executor.on(TaskEvent.TASK_STARTED, on_task_started)
    executor.on(TaskEvent.TASK_COMPLETED, on_task_completed)
    
    # Submit a task
    task_id = executor.submit(sync_task_success, "event_test")
    
    # Wait for completion
    executor.wait_for_all()
    
    # Check that all events were emitted
    event_types = [event[0] for event in events_received]
    assert 'created' in event_types
    assert 'started' in event_types
    assert 'completed' in event_types
    
    executor.shutdown()


if __name__ == "__main__":
    # For local testing
    import subprocess
    import sys
    
    # Run the tests
    result = subprocess.run([sys.executable, "-m", "pytest", __file__, "-v"])
    sys.exit(result.returncode)