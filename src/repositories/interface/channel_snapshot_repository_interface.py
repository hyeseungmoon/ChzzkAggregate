from abc import ABC, abstractmethod
from typing import List
from domain.channel_snapshot import ChannelSnapshot

class ChannelSnapshotRepository(ABC):
    @abstractmethod
    def save_channel_snapshots(self, channel_snapshots: List[ChannelSnapshot]) -> int:
        ...