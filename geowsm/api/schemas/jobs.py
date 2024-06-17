from enum import Enum


SOURCE_REQUIRED_JOB_TYPES = {"graph", "script"}


class JobType(str, Enum):
    regular = "regular"
    graph = "graph"
    script = "script"

    @staticmethod
    def default():
        return JobType.regular

    def requires_source(self):
        return self in SOURCE_REQUIRED_JOB_TYPES
