import heapq
import time

class TaskQueue:
    def __init__(self):
        self._heap = []
        self._counter = 0  # Tie-breaker for same priority

    def _get_priority(self, task):
        # Combined heuristic: earliest deadline first, then shortest runtime
        return (task.deadline, task.est_runtime)

    def enqueue(self, task):
        task.enqueued_time = time.time()
        priority = self._get_priority(task)
        heapq.heappush(self._heap, (priority, self._counter, task))
        self._counter += 1

    def dequeue(self):
        if self._heap:
            return heapq.heappop(self._heap)[-1]
        return None

    def is_empty(self):
        return len(self._heap) == 0

    def __len__(self):
        return len(self._heap)

    def peek_all(self):
        # Return list of tasks ordered by priority without removing them
        return [item[-1] for item in sorted(self._heap)]
