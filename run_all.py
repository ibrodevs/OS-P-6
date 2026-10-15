"""Запустить все демонстрации практической работы №6."""

from barrier_demo import run_demo as run_barrier
from lost_wakeup_demo import run_demo as run_lost_wakeup
from producer_consumer_condition import run_demo as run_condition
from producer_consumer_semaphore import run_demo as run_semaphore
from sync_common import print_result


if __name__ == "__main__":
    print_result("Семафоры + мьютекс", run_semaphore())
    print_result("Мьютекс + Condition", run_condition())

    print("\n=== Барьер ===")
    run_barrier()

    print("\n=== if вместо while вокруг wait() ===")
    for problem in run_lost_wakeup():
        print("Нарушение:", problem)
