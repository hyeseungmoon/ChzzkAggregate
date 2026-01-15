from abc import ABC, abstractmethod
from typing import List
from domain.channel import Channel

class ChannelRepository(ABC):
    @abstractmethod
    def save_channels(self, channels: List[Channel]) -> int:
        ...