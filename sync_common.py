"""Общие компоненты для практической работы №6 по синхронизации."""

from __future__ import annotations

import logging
import threading
from collections import Counter
from dataclasses import dataclass, field
from typing import Iterable


SENTINEL = object()


def make_logger(name: str, filename: str) -> logging.Logger:
    """Создать отдельный логгер нарушений инвариантов для демонстрации."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.ERROR)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.FileHandler(filename, mode="w", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)
    return logger


@dataclass
class ValidationResult:
    expected_count: int
    consumed: list[int]
    violations: list[str]
    max_buffer_size: int

    @property
    def missing(self) -> list[int]:
        expected = set(range(self.expected_count))
        return sorted(expected - set(self.consumed))

    @property
    def duplicates(self) -> dict[int, int]:
        counts = Counter(self.consumed)
        return {item: count for item, count in counts.items() if count > 1}

    @property
    def unexpected(self) -> list[int]:
        return sorted({x for x in self.consumed if x < 0 or x >= self.expected_count})

    @property
    def ok(self) -> bool:
        return (
            len(self.consumed) == self.expected_count
            and not self.missing
            and not self.duplicates
            and not self.unexpected
            and not self.violations
            and self.max_buffer_size <= 5
        )


@dataclass
class ConsumptionRecorder:
    expected_count: int
    logger: logging.Logger
    consumed: list[int] = field(default_factory=list)
    violations: list[str] = field(default_factory=list)
    _seen: set[int] = field(default_factory=set, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def violation(self, message: str) -> None:
        with self._lock:
            self.violations.append(message)
            self.logger.error(message)

    def record(self, item: int) -> None:
        with self._lock:
            if item in self._seen:
                message = f"Элемент {item} потреблён повторно"
                self.violations.append(message)
                self.logger.error(message)
            self._seen.add(item)
            self.consumed.append(item)

    def finish(self, max_buffer_size: int) -> ValidationResult:
        result = ValidationResult(
            expected_count=self.expected_count,
            consumed=list(self.consumed),
            violations=list(self.violations),
            max_buffer_size=max_buffer_size,
        )
        if result.missing:
            self.violation(f"Не потреблены элементы: {result.missing[:20]}")
        if result.unexpected:
            self.violation(f"Получены неожиданные элементы: {result.unexpected[:20]}")

        return ValidationResult(
            expected_count=self.expected_count,
            consumed=list(self.consumed),
            violations=list(self.violations),
            max_buffer_size=max_buffer_size,
        )


def producer_items(producer_id: int, producers: int, total_items: int) -> Iterable[int]:
    """Раздать 0..total_items-1 между производителями без общего счётчика."""
    return range(producer_id, total_items, producers)


def print_result(title: str, result: ValidationResult) -> None:
    print(f"\n=== {title} ===")
    print(f"Потреблено элементов : {len(result.consumed)}")
    print(f"Пропущено            : {len(result.missing)}")
    print(f"Дубликатов            : {len(result.duplicates)}")
    print(f"Нарушений инвариантов : {len(result.violations)}")
    print(f"Макс. размер буфера   : {result.max_buffer_size}/5")
    print(f"Результат             : {'OK' if result.ok else 'ERROR'}")
