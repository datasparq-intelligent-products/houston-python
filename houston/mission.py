
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime


@dataclass
class MissionStage(object):
    """MissionStage represents a stage within a mission.
    It has the same attributes as stages in a plan and additional stateful attributes.
    """
    name: str
    service: str
    upstream: List[str]
    downstream: List[str]
    params: dict
    state: int
    start: datetime
    end: datetime

    def __init__(self, data: dict):
        self.name = data["n"]
        self.service = data["a"]
        self.upstream = data["u"]
        self.downstream = data["d"]
        self.params = data["p"] if data["p"] is not None else dict()
        self.state = data["s"]
        self.start = datetime.strptime(data["t"], "%Y-%m-%dT%H:%M:%S.%fZ")
        self.end = datetime.strptime(data["e"], "%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass
class Mission(object):
    """Mission class
    """
    id: str
    name: str
    services: List[object]
    stages: List[MissionStage]
    start: datetime
    end: datetime
    params: dict

    def __init__(self, data: dict):
        self.id = data["i"]
        self.name = data["n"]
        self.services = data["a"]
        self.stages = [MissionStage(s) for s in data["s"]]
        self.start = datetime.strptime(data["t"], "%Y-%m-%dT%H:%M:%S.%fZ")
        self.end = datetime.strptime(data["e"], "%Y-%m-%dT%H:%M:%S.%fZ")
        self.params = data["p"] if data["p"] is not None else dict()

    def get_stage(self, stage_name: str) -> Optional[MissionStage]:
        """Find a stage within a mission. Returns `None` if the stage doesn't exist.

        :param stage_name: The name of the stage
        """
        filtered_stages = [s for s in self.stages if s.name == stage_name]

        if len(filtered_stages) == 0:
            return None
        else:
            return filtered_stages[0]
