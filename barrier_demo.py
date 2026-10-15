"""Многоразовый барьер на Condition для 4 потоков и 3 фаз."""

from __future__ import annotations

import random
import threading
import time

THREADS = 4
PHASES = 3


class ConditionBarrier:
    """Циклический барьер, реализованный без threading.Barrier."""

    def __init__(self, parties: int) -> None:
        if parties <= 0:
            raise ValueError("parties must be > 0")
        self.parties = parties
        self.count = 0
        self.generation = 0
        self.condition = threading.Condition()

    def wait(self) -> None:
        with self.condition:
            my_generation = self.generation
            self.count += 1

            if self.count == self.parties:
                self.count = 0
                self.generation += 1
                self.condition.notify_all()
                return

            while my_generation == self.generation:
                self.condition.wait()


def run_demo() -> list[list[int]]:
    barrier = ConditionBarrier(THREADS)
    completed_by_phase: list[list[int]] = [[] for _ in range(PHASES)]
    lock = threading.Lock()

    def worker(worker_id: int) -> None:
        for phase in range(PHASES):
            time.sleep(random.uniform(0.001, 0.01))
            with lock:
                completed_by_phase[phase].append(worker_id)
                print(f"Поток {worker_id}: закончил фазу {phase + 1}")

            barrier.wait()
            print(f"Поток {worker_id}: прошёл барьер фазы {phase + 1}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(THREADS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert all(sorted(done) == list(range(THREADS)) for done in completed_by_phase)
    assert barrier.count == 0
    assert barrier.generation == PHASES
    return completed_by_phase


if __name__ == "__main__":
    phases = run_demo()
    print("\nБарьер корректен:", phases)
