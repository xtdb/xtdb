from datetime import datetime
from enum import Enum


class TemporalFilter:
    def to_json(self):
        pass


class From(TemporalFilter):
    def __init__(self, from_: datetime):
        self.from_ = from_

    def to_json(self):
        return {"from": self.from_.isoformat()}


class To(TemporalFilter):
    def __init__(self, to: datetime):
        self.to = to

    def to_json(self):
        return {"to": self.to.isoformat()}


class In(TemporalFilter):
    def __init__(self, from_: datetime, to: datetime):
        self.from_ = from_
        self.to = to

    def to_json(self):
        return {"from": self.from_.isoformat(), "to": self.to.isoformat()}


class AllTime(TemporalFilter, Enum):
    AllTime = "allTime"

    def to_json(self):
        return "allTime"


