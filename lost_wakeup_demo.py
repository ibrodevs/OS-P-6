"""Демонстрация, почему Condition.wait() нужно окружать while, а не if.

Два потребителя ждут один элемент. Производитель делает notify_all(). Оба
просыпаются, но элемент только один. Первый забирает его, а второй в ошибочной
версии с if не проверяет условие повторно и фиксирует underflow.
"""

from __future__ import annotations

import threading
import time


class BrokenOneSlotBuffer:
    def __init__(self) -> None:
        self.item: int | None = None
        self.condition = threading.Condition()
        self.waiting = 0
        self.violations: list[str] = []

    def bad_get(self) -> int | None:
        with self.condition:
            # ОШИБКА: после wake-up условие не проверяется повторно.
            if self.item is None:
                self.waiting += 1
                try:
                    self.condition.wait()
                finally:
                    self.waiting -= 1

            if self.item is None:
                self.violations.append(
                    "Поток проснулся, но элемент уже забрал другой потребитель"
                )
                return None

            value = self.item
            self.item = None
            return value

    def put_and_wake_all(self, value: int) -> None:
        with self.condition:
            self.item = value
            self.condition.notify_all()


def run_demo() -> list[str]:
    buffer = BrokenOneSlotBuffer()
    results: list[int | None] = []
    results_lock = threading.Lock()

    def consumer() -> None:
        value = buffer.bad_get()
        with results_lock:
            results.append(value)

    threads = [threading.Thread(target=consumer, name=f"consumer-{i}") for i in range(2)]
    for thread in threads:
        thread.start()

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        with buffer.condition:
            if buffer.waiting == 2:
                break
        time.sleep(0.001)
    else:
        raise RuntimeError("Потребители не успели перейти в wait()")

    buffer.put_and_wake_all(42)

    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive(), "Поток завис"

    assert sorted(results, key=lambda x: x is None) == [42, None]
    assert buffer.violations, "Ожидалось нарушение из-за if вместо while"
    return buffer.violations


if __name__ == "__main__":
    violations = run_demo()
    print("Нарушение воспроизведено:")
    for violation in violations:
        print(" -", violation)
    print("\nИсправление: использовать while <условие не выполнено>: condition.wait()")
