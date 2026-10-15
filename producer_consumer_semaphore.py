"""Producer-consumer: кольцевой буфер на семафорах и мьютексе.

3 производителя, 2 потребителя, 1000 полезных элементов, capacity=5.
queue.Queue намеренно не используется.
"""

from __future__ import annotations

import threading
from typing import Any

from sync_common import (
    SENTINEL,
    ConsumptionRecorder,
    ValidationResult,
    make_logger,
    print_result,
    producer_items,
)

CAPACITY = 5
PRODUCERS = 3
CONSUMERS = 2
TOTAL_ITEMS = 1000


class SemaphoreRingBuffer:
    def __init__(self, capacity: int, recorder: ConsumptionRecorder) -> None:
        self.capacity = capacity
        self.buffer: list[Any | None] = [None] * capacity
        self.head = 0
        self.tail = 0
        self.count = 0
        self.max_size = 0

        self.empty = threading.Semaphore(capacity)
        self.full = threading.Semaphore(0)
        self.mutex = threading.Lock()
        self.recorder = recorder

    def _check_invariants(self) -> None:
        if not (0 <= self.count <= self.capacity):
            self.recorder.violation(
                f"Нарушен размер буфера: count={self.count}, capacity={self.capacity}"
            )
        if not (0 <= self.head < self.capacity and 0 <= self.tail < self.capacity):
            self.recorder.violation(
                f"Индекс вне кольца: head={self.head}, tail={self.tail}"
            )

    def put(self, item: Any) -> None:
        self.empty.acquire()
        with self.mutex:
            if self.count >= self.capacity:
                self.recorder.violation("Переполнение буфера в semaphore.put()")
            self.buffer[self.tail] = item
            self.tail = (self.tail + 1) % self.capacity
            self.count += 1
            self.max_size = max(self.max_size, self.count)
            self._check_invariants()
        self.full.release()

    def get(self) -> Any:
        self.full.acquire()
        with self.mutex:
            if self.count <= 0:
                self.recorder.violation("Опустошение буфера в semaphore.get()")
            item = self.buffer[self.head]
            self.buffer[self.head] = None
            self.head = (self.head + 1) % self.capacity
            self.count -= 1
            self._check_invariants()
        self.empty.release()
        return item


def run_demo(total_items: int = TOTAL_ITEMS) -> ValidationResult:
    logger = make_logger("semaphore_buffer", "semaphore_violations.log")
    recorder = ConsumptionRecorder(total_items, logger)
    buffer = SemaphoreRingBuffer(CAPACITY, recorder)

    def producer(pid: int) -> None:
        for item in producer_items(pid, PRODUCERS, total_items):
            buffer.put(item)

    def consumer() -> None:
        while True:
            item = buffer.get()
            if item is SENTINEL:
                return
            recorder.record(item)

    consumers = [threading.Thread(target=consumer, name=f"consumer-{i}") for i in range(CONSUMERS)]
    producers = [
        threading.Thread(target=producer, args=(i,), name=f"producer-{i}")
        for i in range(PRODUCERS)
    ]

    for thread in consumers + producers:
        thread.start()
    for thread in producers:
        thread.join()

    for _ in range(CONSUMERS):
        buffer.put(SENTINEL)
    for thread in consumers:
        thread.join()

    result = recorder.finish(buffer.max_size)
    assert result.ok, (
        f"Проверка не пройдена: missing={result.missing}, "
        f"duplicates={result.duplicates}, violations={result.violations}"
    )
    return result


if __name__ == "__main__":
    print_result("Семафоры + мьютекс", run_demo())
