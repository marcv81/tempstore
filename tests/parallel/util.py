import threading

# Thread-safe counter.
# Only used for parallel testing.
class Counter:

    def __init__(self):
        self.counter = 0
        self.lock = threading.Lock()

    def increment(self):
        with self.lock:
            self.counter += 1

    def value(self):
        with self.lock:
            return self.counter
