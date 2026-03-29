# Task Scheduler — DAG-based Execution Engine

A production-ready task scheduler with dependency resolution, priority queuing, and retry logic. Executes tasks in correct topological order while respecting priorities. Built with pure Python — zero external dependencies.

## Features

- **DAG dependency resolution** — topological sort ensures correct execution order
- **Priority queuing** — when multiple tasks are ready, highest priority runs first
- **Retry logic** — configurable retry attempts on task failure
- **Cascade skip** — when a task fails, all dependents are automatically skipped
- **Cycle detection** — raises `CyclicDependencyError` for circular dependencies
- **Handler return values** — capture and retrieve results from completed tasks
- **Type-safe** — passes `mypy --strict` with zero errors
- **Battle-tested** — 41/41 tests passing
- **Zero dependencies** — pure Python standard library

## Installation

```bash
git clone https://github.com/YOUR_USERNAME/task-scheduler.git
cd task-scheduler
```

## Quick Start

```python
from task_scheduler import TaskScheduler, TaskStatus

scheduler = TaskScheduler()

# Add tasks with dependencies
scheduler.add_task("fetch_data", priority=0, handler=lambda: {"users": 100})
scheduler.add_task("validate", priority=1, dependencies=["fetch_data"],
                   handler=lambda: True)
scheduler.add_task("transform", priority=2, dependencies=["validate"],
                   handler=lambda: "transformed")
scheduler.add_task("load", priority=3, dependencies=["transform"],
                   handler=lambda: "done")

# Preview execution order
print(scheduler.get_execution_order())
# ['fetch_data', 'validate', 'transform', 'load']

# Execute all tasks
results = scheduler.execute()
for r in results:
    print(f"{r.task_id}: {r.status.value} (attempts: {r.attempts})")

# Check individual status
print(scheduler.get_status("load"))  # TaskStatus.COMPLETED
print(scheduler.get_task_result("fetch_data"))  # {'users': 100}
```

### Retry Logic

```python
attempt_count = 0

def unreliable_api():
    global attempt_count
    attempt_count += 1
    if attempt_count < 3:
        raise ConnectionError("API timeout")
    return {"status": "ok"}

scheduler = TaskScheduler()
scheduler.add_task("api_call", max_retries=3, handler=unreliable_api)
results = scheduler.execute()
# Succeeds on 3rd attempt
```

### Cascade Skip on Failure

```python
scheduler = TaskScheduler()
scheduler.add_task("database", handler=lambda: (_ for _ in ()).throw(
    RuntimeError("DB down")))
scheduler.add_task("process", dependencies=["database"])
scheduler.add_task("report", dependencies=["process"])

results = scheduler.execute()
# database: FAILED
# process:  SKIPPED (dependency 'database' failed)
# report:   SKIPPED (dependency 'process' failed)
```

## API Reference

### `TaskScheduler()`

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `add_task(task_id, priority, dependencies, max_retries, handler)` | `None` | Register a new task |
| `execute()` | `List[TaskResult]` | Run all tasks in order |
| `get_execution_order()` | `List[str]` | Preview order without executing |
| `get_status(task_id)` | `TaskStatus` | Current task status |
| `get_task_result(task_id)` | `Any` | Handler return value |
| `reset()` | `None` | Reset all tasks to PENDING |
| `clear()` | `None` | Remove all tasks |

### `add_task` Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task_id` | `str` | required | Unique task identifier |
| `priority` | `int` | `5` | 0 (highest) to 10 (lowest) |
| `dependencies` | `List[str]` | `[]` | Task IDs that must complete first |
| `max_retries` | `int` | `0` | Retry attempts on failure |
| `handler` | `Callable` | no-op | Function to execute |

### Task Statuses

| Status | Description |
|--------|-------------|
| `PENDING` | Not yet executed |
| `RUNNING` | Currently executing |
| `COMPLETED` | Successfully finished |
| `FAILED` | All attempts exhausted |
| `SKIPPED` | Dependency failed |

### Exceptions

| Exception | When |
|-----------|------|
| `CyclicDependencyError` | Circular dependencies detected |
| `DuplicateTaskError` | Task ID already exists |
| `TaskNotFoundError` | Referencing non-existent task |

## Running Tests

```bash
pip install pytest
pytest test_task_scheduler.py -v
```

### Test Results

```
41 passed in 0.17s
```

### Test Coverage

| Category | Tests | Description |
|----------|-------|-------------|
| Task Creation | 10 | Validation, duplicates, types |
| Execution Order | 6 | Topological sort, priorities, diamonds |
| Cycle Detection | 4 | Simple, 3-node, self, partial cycles |
| Missing Dependencies | 2 | Non-existent task references |
| Retry Logic | 4 | Succeed after retry, exhaustion |
| Cascade Skip | 3 | Simple, deep chain, partial failure |
| Reset & Clear | 4 | State management, re-execution |
| Handler Results | 3 | Return values, pending, not found |
| Performance | 3 | 100-chain, 1000-wide, complex DAG |
| Repr | 2 | String representation |

## Architecture

```
TaskScheduler
├── add_task()              # Register task with deps & priority
├── get_execution_order()   # Kahn's algorithm + priority heap
├── execute()               # Run tasks with retry & cascade skip
│   ├── _validate_dependencies()
│   ├── _detect_cycles()
│   └── _cascade_skip()     # Recursive dependent skipping
├── get_status()            # Query task state
├── get_task_result()       # Get handler return value
├── reset()                 # Reset to PENDING
└── clear()                 # Remove all tasks
```

**Algorithm:** Kahn's topological sort with a min-heap priority queue. When multiple tasks have zero in-degree (all dependencies met), the one with highest priority (lowest number) executes first.

## License

MIT
