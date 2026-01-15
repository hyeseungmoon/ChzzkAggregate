from abc import ABC, abstractmethod
from typing import List
from domain.live_snapshot import LiveSnapshot

class LiveSnapshotRepository(ABC):
    @abstractmethod
    def save_live_snapshots(self, live_snapshots: List[LiveSnapshot]) -> int:
        ...