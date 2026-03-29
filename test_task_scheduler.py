"""
Comprehensive tests for TaskScheduler.
Covers: creation, dependencies, priorities, retry, cascade skip,
cycle detection, edge cases, performance.
"""

import pytest
from task_scheduler import (
    TaskScheduler,
    TaskStatus,
    CyclicDependencyError,
    DuplicateTaskError,
    TaskNotFoundError,
)


# ============================================================
# 1. Task Creation & Validation
# ============================================================

class TestTaskCreation:
    """Tests for adding tasks and input validation."""

    def test_add_simple_task(self):
        s = TaskScheduler()
        s.add_task("t1", handler=lambda: "done")
        assert s.task_count == 1
        assert s.get_status("t1") == TaskStatus.PENDING

    def test_add_task_default_priority(self):
        s = TaskScheduler()
        s.add_task("t1")
        assert s.get_status("t1") == TaskStatus.PENDING

    def test_add_task_no_handler(self):
        s = TaskScheduler()
        s.add_task("t1")
        results = s.execute()
        assert results[0].status == TaskStatus.COMPLETED

    def test_duplicate_task_raises(self):
        s = TaskScheduler()
        s.add_task("t1")
        with pytest.raises(DuplicateTaskError):
            s.add_task("t1")

    def test_invalid_priority_high(self):
        s = TaskScheduler()
        with pytest.raises(ValueError):
            s.add_task("t1", priority=11)

    def test_invalid_priority_negative(self):
        s = TaskScheduler()
        with pytest.raises(ValueError):
            s.add_task("t1", priority=-1)

    def test_invalid_priority_type(self):
        s = TaskScheduler()
        with pytest.raises(ValueError):
            s.add_task("t1", priority="high")  # type: ignore[arg-type]

    def test_invalid_retries(self):
        s = TaskScheduler()
        with pytest.raises(ValueError):
            s.add_task("t1", max_retries=-1)

    def test_invalid_handler(self):
        s = TaskScheduler()
        with pytest.raises(TypeError):
            s.add_task("t1", handler="not_callable")  # type: ignore[arg-type]

    def test_task_not_found(self):
        s = TaskScheduler()
        with pytest.raises(TaskNotFoundError):
            s.get_status("nonexistent")


# ============================================================
# 2. Execution Order & Dependencies
# ============================================================

class TestExecutionOrder:
    """Tests for topological sorting and dependency handling."""

    def test_no_dependencies(self):
        s = TaskScheduler()
        s.add_task("a", priority=2)
        s.add_task("b", priority=0)
        s.add_task("c", priority=1)
        order = s.get_execution_order()
        assert order == ["b", "c", "a"]

    def test_linear_chain(self):
        s = TaskScheduler()
        s.add_task("step3", dependencies=["step2"])
        s.add_task("step1")
        s.add_task("step2", dependencies=["step1"])
        order = s.get_execution_order()
        assert order == ["step1", "step2", "step3"]

    def test_diamond_dependency(self):
        """A -> B, A -> C, B -> D, C -> D."""
        s = TaskScheduler()
        s.add_task("A", priority=0)
        s.add_task("B", priority=1, dependencies=["A"])
        s.add_task("C", priority=2, dependencies=["A"])
        s.add_task("D", priority=0, dependencies=["B", "C"])
        order = s.get_execution_order()
        assert order.index("A") < order.index("B")
        assert order.index("A") < order.index("C")
        assert order.index("B") < order.index("D")
        assert order.index("C") < order.index("D")

    def test_priority_within_same_level(self):
        s = TaskScheduler()
        s.add_task("low", priority=10)
        s.add_task("high", priority=0)
        s.add_task("mid", priority=5)
        order = s.get_execution_order()
        assert order == ["high", "mid", "low"]

    def test_empty_scheduler(self):
        s = TaskScheduler()
        assert s.get_execution_order() == []
        assert s.execute() == []

    def test_single_task(self):
        s = TaskScheduler()
        s.add_task("only", handler=lambda: 42)
        results = s.execute()
        assert len(results) == 1
        assert results[0].status == TaskStatus.COMPLETED
        assert results[0].result == 42


# ============================================================
# 3. Cycle Detection
# ============================================================

class TestCycleDetection:
    """Tests for circular dependency detection."""

    def test_simple_cycle(self):
        s = TaskScheduler()
        s.add_task("a", dependencies=["b"])
        s.add_task("b", dependencies=["a"])
        with pytest.raises(CyclicDependencyError):
            s.get_execution_order()

    def test_three_node_cycle(self):
        s = TaskScheduler()
        s.add_task("a", dependencies=["c"])
        s.add_task("b", dependencies=["a"])
        s.add_task("c", dependencies=["b"])
        with pytest.raises(CyclicDependencyError):
            s.execute()

    def test_self_dependency(self):
        s = TaskScheduler()
        s.add_task("a", dependencies=["a"])
        with pytest.raises(CyclicDependencyError):
            s.get_execution_order()

    def test_partial_cycle(self):
        """Some tasks are fine, but a subset forms a cycle."""
        s = TaskScheduler()
        s.add_task("ok1")
        s.add_task("ok2", dependencies=["ok1"])
        s.add_task("cyc1", dependencies=["cyc2"])
        s.add_task("cyc2", dependencies=["cyc1"])
        with pytest.raises(CyclicDependencyError):
            s.get_execution_order()


# ============================================================
# 4. Missing Dependencies
# ============================================================

class TestMissingDependencies:
    """Tests for referencing non-existent tasks."""

    def test_missing_dependency(self):
        s = TaskScheduler()
        s.add_task("a", dependencies=["ghost"])
        with pytest.raises(TaskNotFoundError):
            s.get_execution_order()

    def test_missing_dependency_on_execute(self):
        s = TaskScheduler()
        s.add_task("a", dependencies=["ghost"])
        with pytest.raises(TaskNotFoundError):
            s.execute()


# ============================================================
# 5. Retry Logic
# ============================================================

class TestRetryLogic:
    """Tests for task retry on failure."""

    def test_retry_then_succeed(self):
        counter = {"n": 0}

        def flaky() -> str:
            counter["n"] += 1
            if counter["n"] < 3:
                raise RuntimeError("not yet")
            return "ok"

        s = TaskScheduler()
        s.add_task("flaky", max_retries=2, handler=flaky)
        results = s.execute()
        assert results[0].status == TaskStatus.COMPLETED
        assert results[0].attempts == 3
        assert results[0].result == "ok"

    def test_retry_exhausted(self):
        def always_fail() -> None:
            raise RuntimeError("boom")

        s = TaskScheduler()
        s.add_task("bad", max_retries=2, handler=always_fail)
        results = s.execute()
        assert results[0].status == TaskStatus.FAILED
        assert results[0].attempts == 3
        assert results[0].error == "boom"

    def test_no_retry(self):
        def fail_once() -> None:
            raise ValueError("oops")

        s = TaskScheduler()
        s.add_task("no_retry", max_retries=0, handler=fail_once)
        results = s.execute()
        assert results[0].status == TaskStatus.FAILED
        assert results[0].attempts == 1

    def test_zero_retries_success(self):
        s = TaskScheduler()
        s.add_task("ok", max_retries=0, handler=lambda: "fine")
        results = s.execute()
        assert results[0].status == TaskStatus.COMPLETED
        assert results[0].attempts == 1


# ============================================================
# 6. Cascade Skip
# ============================================================

class TestCascadeSkip:
    """Tests for skipping dependent tasks when a dependency fails."""

    def test_simple_cascade(self):
        def fail_handler() -> None:
            raise RuntimeError("fail")

        s = TaskScheduler()
        s.add_task("parent", handler=fail_handler)
        s.add_task("child", dependencies=["parent"], handler=lambda: "ok")
        results = s.execute()
        parent_r = next(r for r in results if r.task_id == "parent")
        child_r = next(r for r in results if r.task_id == "child")
        assert parent_r.status == TaskStatus.FAILED
        assert child_r.status == TaskStatus.SKIPPED

    def test_deep_cascade(self):
        """A fails -> B skipped -> C skipped -> D skipped."""
        def fail() -> None:
            raise RuntimeError("root failure")

        s = TaskScheduler()
        s.add_task("A", handler=fail)
        s.add_task("B", dependencies=["A"])
        s.add_task("C", dependencies=["B"])
        s.add_task("D", dependencies=["C"])
        results = s.execute()
        statuses = {r.task_id: r.status for r in results}
        assert statuses["A"] == TaskStatus.FAILED
        assert statuses["B"] == TaskStatus.SKIPPED
        assert statuses["C"] == TaskStatus.SKIPPED
        assert statuses["D"] == TaskStatus.SKIPPED

    def test_partial_failure(self):
        """One branch fails, other branch succeeds."""
        def fail() -> None:
            raise RuntimeError("fail")

        s = TaskScheduler()
        s.add_task("root", priority=0)
        s.add_task("good_branch", priority=1, dependencies=["root"],
                   handler=lambda: "success")
        s.add_task("bad_branch", priority=1, dependencies=["root"],
                   handler=fail)
        s.add_task("after_good", dependencies=["good_branch"])
        s.add_task("after_bad", dependencies=["bad_branch"])
        results = s.execute()
        statuses = {r.task_id: r.status for r in results}
        assert statuses["root"] == TaskStatus.COMPLETED
        assert statuses["good_branch"] == TaskStatus.COMPLETED
        assert statuses["bad_branch"] == TaskStatus.FAILED
        assert statuses["after_good"] == TaskStatus.COMPLETED
        assert statuses["after_bad"] == TaskStatus.SKIPPED


# ============================================================
# 7. Reset & Clear
# ============================================================

class TestResetClear:
    """Tests for reset and clear operations."""

    def test_reset_after_execution(self):
        s = TaskScheduler()
        s.add_task("t1", handler=lambda: "done")
        s.execute()
        assert s.get_status("t1") == TaskStatus.COMPLETED
        s.reset()
        assert s.get_status("t1") == TaskStatus.PENDING

    def test_reset_preserves_tasks(self):
        s = TaskScheduler()
        s.add_task("t1")
        s.add_task("t2")
        s.execute()
        s.reset()
        assert s.task_count == 2

    def test_clear_removes_all(self):
        s = TaskScheduler()
        s.add_task("t1")
        s.add_task("t2")
        s.clear()
        assert s.task_count == 0

    def test_re_execute_after_reset(self):
        counter = {"n": 0}

        def increment() -> int:
            counter["n"] += 1
            return counter["n"]

        s = TaskScheduler()
        s.add_task("counter", handler=increment)
        s.execute()
        assert s.get_task_result("counter") == 1
        s.reset()
        s.execute()
        assert s.get_task_result("counter") == 2


# ============================================================
# 8. Handler Return Values
# ============================================================

class TestHandlerResults:
    """Tests for capturing handler return values."""

    def test_result_captured(self):
        s = TaskScheduler()
        s.add_task("calc", handler=lambda: 42 * 2)
        s.execute()
        assert s.get_task_result("calc") == 84

    def test_result_none_for_pending(self):
        s = TaskScheduler()
        s.add_task("pending")
        assert s.get_task_result("pending") is None

    def test_result_not_found(self):
        s = TaskScheduler()
        with pytest.raises(TaskNotFoundError):
            s.get_task_result("ghost")


# ============================================================
# 9. Performance
# ============================================================

class TestPerformance:
    """Stress tests for large task graphs."""

    def test_hundred_tasks_chain(self):
        s = TaskScheduler()
        for i in range(100):
            deps = [f"task_{i-1}"] if i > 0 else []
            s.add_task(f"task_{i}", dependencies=deps, handler=lambda: True)
        results = s.execute()
        assert all(r.status == TaskStatus.COMPLETED for r in results)
        assert len(results) == 100

    def test_wide_graph(self):
        """1000 independent tasks."""
        s = TaskScheduler()
        for i in range(1000):
            s.add_task(f"t_{i}", priority=i % 11, handler=lambda: True)
        results = s.execute()
        assert all(r.status == TaskStatus.COMPLETED for r in results)

    def test_complex_dag(self):
        """Multiple layers with cross-dependencies."""
        s = TaskScheduler()
        # Layer 0: 3 root tasks
        for i in range(3):
            s.add_task(f"root_{i}", priority=i)
        # Layer 1: each depends on all roots
        for i in range(5):
            s.add_task(f"mid_{i}", priority=i,
                       dependencies=[f"root_{j}" for j in range(3)])
        # Layer 2: each depends on all mids
        for i in range(3):
            s.add_task(f"leaf_{i}", priority=i,
                       dependencies=[f"mid_{j}" for j in range(5)])

        order = s.get_execution_order()
        # All roots before all mids
        root_indices = [order.index(f"root_{i}") for i in range(3)]
        mid_indices = [order.index(f"mid_{i}") for i in range(5)]
        leaf_indices = [order.index(f"leaf_{i}") for i in range(3)]

        assert max(root_indices) < min(mid_indices)
        assert max(mid_indices) < min(leaf_indices)


# ============================================================
# 10. Repr
# ============================================================

class TestRepr:
    """Test string representation."""

    def test_repr_empty(self):
        s = TaskScheduler()
        assert "tasks=0" in repr(s)

    def test_repr_with_tasks(self):
        s = TaskScheduler()
        s.add_task("t1")
        s.add_task("t2")
        r = repr(s)
        assert "tasks=2" in r
        assert "pending" in r
