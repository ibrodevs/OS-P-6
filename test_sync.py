"""Автоматические проверки инвариантов практической работы №6."""

import unittest

from barrier_demo import PHASES, THREADS, run_demo as run_barrier
from condition_while_fixed import run_demo as run_fixed
from lost_wakeup_demo import run_demo as run_broken
from producer_consumer_condition import run_demo as run_condition
from producer_consumer_semaphore import run_demo as run_semaphore


class SynchronizationTests(unittest.TestCase):
    def test_semaphore_buffer(self) -> None:
        result = run_semaphore()
        self.assertTrue(result.ok)
        self.assertEqual(len(result.consumed), 1000)
        self.assertLessEqual(result.max_buffer_size, 5)

    def test_condition_buffer(self) -> None:
        result = run_condition()
        self.assertTrue(result.ok)
        self.assertEqual(len(result.consumed), 1000)
        self.assertLessEqual(result.max_buffer_size, 5)

    def test_barrier_three_phases(self) -> None:
        phases = run_barrier()
        self.assertEqual(len(phases), PHASES)
        for completed in phases:
            self.assertEqual(sorted(completed), list(range(THREADS)))

    def test_fixed_while_handles_two_consumers(self) -> None:
        self.assertEqual(sorted(run_fixed()), [42, 43])

    def test_broken_if_reproduces_violation(self) -> None:
        violations = run_broken()
        self.assertGreaterEqual(len(violations), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
