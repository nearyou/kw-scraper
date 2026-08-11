"""Contiguous progress tracking for out-of-order worker completion."""


class ContiguousProgressTracker:
    """Advance a checkpoint only when every preceding number has completed."""

    def __init__(self, start_number):
        self.next_number = int(start_number)
        self.completed = set()
        self.last_checkpoint = self.next_number - 1

    def mark_completed(self, number):
        number = int(number)
        if number < self.next_number:
            return self.last_checkpoint, 0
        self.completed.add(number)
        advanced = 0
        while self.next_number in self.completed:
            self.completed.remove(self.next_number)
            self.last_checkpoint = self.next_number
            self.next_number += 1
            advanced += 1
        return self.last_checkpoint, advanced
