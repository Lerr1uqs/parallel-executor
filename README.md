# Parallel Executor

A parallel execution library supporting both async and sync task execution with event emission and callbacks.

## Features

- **Async and Sync Execution**: Run tasks concurrently using either asyncio or threading
- **Event Emission**: Listen for task events (created, started, completed, failed)
- **Callback Support**: Execute callbacks when tasks complete
- **Type Safety**: Full type hints and generic support
- **Task Management**: Track task status, results, and errors
- **Pydantic Models**: Structured data models for task information

## Installation

```bash
pip install parallel-executor
```

## Usage

### Async Executor

```python
import asyncio
from parallel_executor.asynchronous import ParallelExecutor
from parallel_executor.events import TaskEvent

async def fetch_data(source: str) -> str:
    await asyncio.sleep(1)  # Simulate network request
    return f"data_from_{source}"

async def main():
    executor = ParallelExecutor()
    
    # Register event listeners using TaskEvent enum
    executor.on(TaskEvent.TASK_CREATED, lambda info: print(f"Task {info.task_id} created"))
    executor.on(TaskEvent.TASK_STARTED, lambda info: print(f"Task {info.task_id} started"))  
    executor.on(TaskEvent.TASK_COMPLETED, lambda info, result: print(f"Task {info.task_id} completed"))
    
    # Submit tasks
    task_id = executor.submit(fetch_data, "API_1")
    
    # Wait for completion
    results = await executor.wait_for_all()
    
    print(results[0].result)  # "data_from_API_1"

if __name__ == "__main__":
    asyncio.run(main())
```

### Sync Executor

```python
from parallel_executor.synchronous import ParallelExecutor
from parallel_executor.events import TaskEvent
import time

def process_data(data: str) -> str:
    time.sleep(1)  # Simulate work
    return f"processed_{data}"

def main():
    executor = ParallelExecutor()
    
    # Register event listeners using TaskEvent enum
    executor.on(TaskEvent.TASK_STARTED, lambda info: print(f"Task {info.task_id} started"))
    executor.on(TaskEvent.TASK_COMPLETED, lambda info, result: print(f"Task {info.task_id} completed"))
    
    # Submit a task
    task_id = executor.submit(process_data, "important_data")
    
    # Wait for completion
    results = executor.wait_for_all()
    
    print(results[0].result)  # "processed_important_data"
    
    # Clean up
    executor.shutdown()

if __name__ == "__main__":
    main()
```

### Using Callbacks

You can also submit tasks with callbacks:

```python
from parallel_executor.synchronous import ParallelExecutor

def process_data(data: str) -> str:
    return f"processed_{data}"

def on_completion(result: str):
    print(f"Task completed with result: {result}")

def main():
    executor = ParallelExecutor()
    
    # Submit a task with callback
    task_id = executor.submit(process_data, "important_data", callback=on_completion)
    
    # Wait for completion
    executor.wait_for_all()

if __name__ == "__main__":
    main()
```

## Running Tests

```bash
python -m pytest test_executor.py -v
```

## Running Examples

```bash
python main.py
```


## mypy
```
uv add mypy-stubgen
stubgen -p parallel_executor -o stubs
mypy main.py 
```

## coverage
todo

## pytest
```
python -m pytest test_executor.py -v 
```