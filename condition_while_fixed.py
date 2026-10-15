"""Исправленная версия примера с Condition: wait() всегда внутри while."""

from __future__ import annotations

import threading
import time


class FixedOneSlotBuffer:
    def __init__(self) -> None:
        self.item: int | None = None
        self.condition = threading.Condition()
        self.waiting = 0

    def get(self) -> int:
        with self.condition:
            while self.item is None:
                self.waiting += 1
                try:
                    self.condition.wait()
                finally:
                    self.waiting -= 1

            value = self.item
            self.item = None
            self.condition.notify_all()
            return value

    def put(self, value: int) -> None:
        with self.condition:
            while self.item is not None:
                self.condition.wait()
            self.item = value
            self.condition.notify_all()


def run_demo() -> list[int]:
    buffer = FixedOneSlotBuffer()
    results: list[int] = []
    result_lock = threading.Lock()

    def consumer() -> None:
        value = buffer.get()
        with result_lock:
            results.append(value)

    consumers = [threading.Thread(target=consumer) for _ in range(2)]
    for thread in consumers:
        thread.start()

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        with buffer.condition:
            if buffer.waiting == 2:
                break
        time.sleep(0.001)
    else:
        raise RuntimeError("Потребители не перешли в wait()")

    buffer.put(42)
    buffer.put(43)

    for thread in consumers:
        thread.join(timeout=2)
        assert not thread.is_alive()

    assert sorted(results) == [42, 43]
    return results


if __name__ == "__main__":
    print("Получено без нарушений:", sorted(run_demo()))
