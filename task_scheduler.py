"""
Task Scheduler — DAG-based Task Execution Engine
==================================================
Manages task execution with priorities, dependencies, and retry logic.
Topological ordering ensures correct execution sequence.

Author: Vlad M.
"""

from __future__ import annotations

import enum
import heapq
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


class TaskStatus(enum.Enum):
    """Possible states of a task."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class CyclicDependencyError(Exception):
    """Raised when circular dependencies are detected."""


class DuplicateTaskError(Exception):
    """Raised when a task with the same ID already exists."""


class TaskNotFoundError(Exception):
    """Raised when referencing a non-existent task."""


@dataclass
class TaskResult:
    """Result of a single task execution."""

    task_id: str
    status: TaskStatus
    attempts: int
    error: Optional[str] = None
    result: Any = None


@dataclass
class Task:
    """Internal representation of a scheduled task."""

    task_id: str
    priority: int
    dependencies: List[str]
    max_retries: int
    handler: Callable[[], Any]
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    last_error: Optional[str] = None
    result: Any = None


class TaskScheduler:
    """
    DAG-based task scheduler with priorities and retry logic.

    Tasks are executed in topological order respecting dependencies.
    When multiple tasks are ready simultaneously, higher priority
    (lower number) tasks execute first.

    Attributes:
        tasks: Dictionary of registered tasks.
    """

    def __init__(self) -> None:
        self._tasks: Dict[str, Task] = {}

    @property
    def task_count(self) -> int:
        """Number of registered tasks."""
        return len(self._tasks)

    def add_task(
        self,
        task_id: str,
        priority: int = 5,
        dependencies: Optional[List[str]] = None,
        max_retries: int = 0,
        handler: Optional[Callable[[], Any]] = None,
    ) -> None:
        """
        Register a new task.

        Args:
            task_id: Unique identifier for the task.
            priority: Execution priority (0=highest, 10=lowest).
            dependencies: List of task IDs that must complete first.
            max_retries: Number of retry attempts on failure.
            handler: Callable to execute. Defaults to no-op.

        Raises:
            DuplicateTaskError: If task_id already exists.
            ValueError: If priority is out of range or max_retries < 0.
            TypeError: If handler is not callable.
        """
        if task_id in self._tasks:
            raise DuplicateTaskError(
                f"Task '{task_id}' already exists"
            )

        if not isinstance(priority, int) or not (0 <= priority <= 10):
            raise ValueError(
                f"Priority must be int 0-10, got {priority}"
            )

        if not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError(
                f"max_retries must be non-negative int, got {max_retries}"
            )

        if handler is not None and not callable(handler):
            raise TypeError(
                f"handler must be callable, got {type(handler).__name__}"
            )

        actual_handler: Callable[[], Any] = handler if handler is not None else lambda: None

        self._tasks[task_id] = Task(
            task_id=task_id,
            priority=priority,
            dependencies=list(dependencies) if dependencies else [],
            max_retries=max_retries,
            handler=actual_handler,
        )

    def _validate_dependencies(self) -> None:
        """
        Check that all dependencies reference existing tasks.

        Raises:
            TaskNotFoundError: If a dependency references a non-existent task.
        """
        for task in self._tasks.values():
            for dep_id in task.dependencies:
                if dep_id not in self._tasks:
                    raise TaskNotFoundError(
                        f"Task '{task.task_id}' depends on "
                        f"non-existent task '{dep_id}'"
                    )

    def _build_graph(self) -> tuple[Dict[str, int], Dict[str, List[str]]]:
        """
        Build in-degree map and dependents map from tasks.

        Returns:
            Tuple of (in_degree dict, dependents dict).
        """
        in_degree: Dict[str, int] = {tid: 0 for tid in self._tasks}
        dependents: Dict[str, List[str]] = {tid: [] for tid in self._tasks}

        for task in self._tasks.values():
            for dep_id in task.dependencies:
                in_degree[task.task_id] += 1
                dependents[dep_id].append(task.task_id)

        return in_degree, dependents

    def _detect_cycles(self) -> None:
        """
        Detect circular dependencies using Kahn's algorithm.

        Raises:
            CyclicDependencyError: If a cycle is found.
        """
        in_degree, dependents = self._build_graph()

        queue: deque[str] = deque(
            tid for tid, deg in in_degree.items() if deg == 0
        )
        visited_count = 0

        while queue:
            current = queue.popleft()
            visited_count += 1

            for dep_tid in dependents[current]:
                in_degree[dep_tid] -= 1
                if in_degree[dep_tid] == 0:
                    queue.append(dep_tid)

        if visited_count != len(self._tasks):
            raise CyclicDependencyError(
                "Circular dependency detected among tasks"
            )

    def get_execution_order(self) -> List[str]:
        """
        Compute execution order using topological sort with priorities.

        Returns:
            Ordered list of task IDs.

        Raises:
            TaskNotFoundError: If dependencies reference missing tasks.
            CyclicDependencyError: If circular dependencies exist.
        """
        if not self._tasks:
            return []

        self._validate_dependencies()
        self._detect_cycles()

        in_degree, dependents = self._build_graph()

        # Priority queue: (priority, task_id)
        ready: List[tuple[int, str]] = []
        for tid, deg in in_degree.items():
            if deg == 0:
                heapq.heappush(ready, (self._tasks[tid].priority, tid))

        order: List[str] = []

        while ready:
            _, current_id = heapq.heappop(ready)
            order.append(current_id)

            for dep_tid in dependents[current_id]:
                in_degree[dep_tid] -= 1
                if in_degree[dep_tid] == 0:
                    heapq.heappush(
                        ready,
                        (self._tasks[dep_tid].priority, dep_tid),
                    )

        return order

    def _cascade_skip(self, failed_id: str, dependents: Dict[str, List[str]]) -> None:
        """
        Recursively skip all tasks that depend on a failed task.

        Args:
            failed_id: The task that failed.
            dependents: Map of task_id -> list of dependent task_ids.
        """
        for dep_tid in dependents.get(failed_id, []):
            task = self._tasks[dep_tid]
            if task.status == TaskStatus.PENDING:
                task.status = TaskStatus.SKIPPED
                task.last_error = (
                    f"Skipped: dependency '{failed_id}' failed"
                )
                self._cascade_skip(dep_tid, dependents)

    def execute(self) -> List[TaskResult]:
        """
        Execute all tasks in dependency/priority order.

        Returns:
            List of TaskResult with execution outcomes.

        Raises:
            TaskNotFoundError: If dependencies reference missing tasks.
            CyclicDependencyError: If circular dependencies exist.
        """
        if not self._tasks:
            return []

        order = self.get_execution_order()

        # Reuse _build_graph for dependents map (cascade skip)
        _, dependents = self._build_graph()

        results: List[TaskResult] = []

        for task_id in order:
            task = self._tasks[task_id]

            # Skip if already marked (cascade skip)
            if task.status == TaskStatus.SKIPPED:
                results.append(TaskResult(
                    task_id=task_id,
                    status=TaskStatus.SKIPPED,
                    attempts=0,
                    error=task.last_error,
                ))
                continue

            # Check if all dependencies completed
            deps_ok = all(
                self._tasks[dep].status == TaskStatus.COMPLETED
                for dep in task.dependencies
            )

            if not deps_ok:
                task.status = TaskStatus.SKIPPED
                task.last_error = "Skipped: dependency not completed"
                self._cascade_skip(task_id, dependents)
                results.append(TaskResult(
                    task_id=task_id,
                    status=TaskStatus.SKIPPED,
                    attempts=0,
                    error=task.last_error,
                ))
                continue

            # Execute with retries
            task.status = TaskStatus.RUNNING
            success = False
            total_attempts = 1 + task.max_retries

            for attempt in range(total_attempts):
                task.attempts = attempt + 1
                try:
                    task.result = task.handler()
                    task.status = TaskStatus.COMPLETED
                    task.last_error = None
                    success = True
                    break
                except Exception as exc:
                    task.last_error = str(exc)

            if not success:
                task.status = TaskStatus.FAILED
                self._cascade_skip(task_id, dependents)

            results.append(TaskResult(
                task_id=task_id,
                status=task.status,
                attempts=task.attempts,
                error=task.last_error,
                result=task.result if success else None,
            ))

        return results

    def get_status(self, task_id: str) -> TaskStatus:
        """
        Get current status of a task.

        Args:
            task_id: Task identifier.

        Returns:
            Current TaskStatus.

        Raises:
            TaskNotFoundError: If task doesn't exist.
        """
        if task_id not in self._tasks:
            raise TaskNotFoundError(f"Task '{task_id}' not found")
        return self._tasks[task_id].status

    def get_task_result(self, task_id: str) -> Optional[Any]:
        """
        Get the return value of a completed task.

        Args:
            task_id: Task identifier.

        Returns:
            Handler return value, or None if not completed.

        Raises:
            TaskNotFoundError: If task doesn't exist.
        """
        if task_id not in self._tasks:
            raise TaskNotFoundError(f"Task '{task_id}' not found")
        return self._tasks[task_id].result

    def reset(self) -> None:
        """Reset all tasks to PENDING status."""
        for task in self._tasks.values():
            task.status = TaskStatus.PENDING
            task.attempts = 0
            task.last_error = None
            task.result = None

    def clear(self) -> None:
        """Remove all tasks."""
        self._tasks.clear()

    def __repr__(self) -> str:
        status_counts: Dict[str, int] = {}
        for task in self._tasks.values():
            name = task.status.value
            status_counts[name] = status_counts.get(name, 0) + 1
        return (
            f"TaskScheduler(tasks={len(self._tasks)}, "
            f"status={status_counts})"
        )
