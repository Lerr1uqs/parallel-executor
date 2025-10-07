import asyncio
from parallel_executor.asynchronous import ParallelExecutor as AsyncParallelExecutor
from parallel_executor.synchronous import ParallelExecutor as SyncParallelExecutor
from parallel_executor.schemas import TaskInfo, TaskResult
from parallel_executor.events import TaskEvent
import time


# Example async functions to run in parallel
async def fetch_data_async(source: str) -> str:
    """Simulate fetching data from a source"""
    print(f"Starting to fetch data from {source}...")
    await asyncio.sleep(1)  # Simulate network request
    result = f"data_from_{source}"
    print(f"Finished fetching data from {source}")
    return result


def process_data_sync(data: str) -> str:
    """Simulate processing data"""
    print(f"Starting to process {data}...")
    time.sleep(1)  # Simulate CPU-intensive work
    result = f"processed_{data}"
    print(f"Finished processing {data}")
    return result


def on_task_completion(result: str):
    """Callback function for when a task completes"""
    print(f"Task completed with result: {result}")


async def example_async_executor():
    """Example usage of the async executor"""
    print("\n=== Async Executor Example ===")
    
    # Create an async executor
    executor = AsyncParallelExecutor()
    
    # Register event listeners
    executor.on(TaskEvent.TASK_STARTED, lambda info: print(f"Task {info.task_id} started"))
    executor.on(TaskEvent.TASK_COMPLETED, lambda info, result: print(f"Task {info.task_id} completed"))
    executor.on(TaskEvent.TASK_FAILED, lambda info, result: print(f"Task {info.task_id} failed: {result.error}"))
    
    # Submit multiple async tasks
    task_ids = []
    sources = ["API_1", "API_2", "API_3"]
    for source in sources:
        task_id = executor.submit(fetch_data_async, source)
        task_ids.append(task_id)
    
    print(f"Submitted {len(task_ids)} async tasks")
    
    # Wait for all tasks to complete
    results = await executor.wait_for_all()
    
    print(f"\nAll async tasks completed. Results: {len(results)}")
    for i, result in enumerate(results):
        if result.completed:
            print(f"  Task {i+1}: {result.result}")
        else:
            print(f"  Task {i+1}: Failed - {result.error}")
    
    # Clean up
    executor.cancel_all()


def example_sync_executor():
    """Example usage of the sync executor"""
    print("\n=== Sync Executor Example ===")
    
    # Create a sync executor
    executor = SyncParallelExecutor()
    
    # Register event listeners
    executor.on(TaskEvent.TASK_STARTED, lambda info: print(f"Task {info.task_id} started"))
    executor.on(TaskEvent.TASK_COMPLETED, lambda info, result: print(f"Task {info.task_id} completed"))
    executor.on(TaskEvent.TASK_FAILED, lambda info, result: print(f"Task {info.task_id} failed: {result.error}"))
    
    # Submit multiple sync tasks
    task_ids = []
    data_list = ["data_1", "data_2", "data_3"]
    for data in data_list:
        task_id = executor.submit(process_data_sync, data)
        task_ids.append(task_id)
    
    print(f"Submitted {len(task_ids)} sync tasks")
    
    # Wait for all tasks to complete
    results = executor.wait_for_all()
    
    print(f"\nAll sync tasks completed. Results: {len(results)}")
    for i, result in enumerate(results):
        if result.completed:
            print(f"  Task {i+1}: {result.result}")
        else:
            print(f"  Task {i+1}: Failed - {result.error}")
    
    # Clean up
    executor.shutdown()


def example_with_callbacks():
    """Example showing how to use callbacks"""
    print("\n=== Executor with Callbacks Example ===")
    
    # Create a sync executor for this example
    executor = SyncParallelExecutor()
    
    # Submit a task with a callback
    task_id = executor.submit(process_data_sync, "important_data", callback=on_task_completion)
    
    print(f"Submitted task {task_id} with callback")
    
    # Wait for the task to complete
    results = executor.wait_for_all()
    
    print(f"Task completed with result: {results[0].result}")
    
    # Clean up
    executor.shutdown()


async def main():
    print("Parallel Executor Examples")
    print("==========================")
    
    # Run async executor example
    await example_async_executor()
    
    # Run sync executor example
    example_sync_executor()
    
    # Run example with callbacks
    example_with_callbacks()
    
    print("\nAll examples completed!")

def main2():
    executor = SyncParallelExecutor()

    def sleep_long():
        time.sleep(5)
    
    def on_sleep_wake(result: TaskResult):
        print(f"Task {result.task_id} completed")
    
    # Submit a task with a callback
    task_id = executor.submit(sleep_long, callback=on_sleep_wake)
    
    print(f"Submitted task {task_id} with callback")
    
    # Wait for the task to complete
    # results = executor.wait_for_all()
    
    # print(f"Task completed with result: {results[0].result}")
    
    # Clean up
    # executor.shutdown()

if __name__ == "__main__":
    # asyncio.run(main())
    main2()
