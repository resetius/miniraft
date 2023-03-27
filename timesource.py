from datetime import *

class TimeSource:
    def now(self):
        return datetime.now()

class FakeTimeSource():
    def __init__(self, cur_time):
        self.cur_time = cur_time

    def now(self):
        return self.cur_time

    def advance(self, delta: timedelta):
        self.cur_time = self.cur_time + delta
