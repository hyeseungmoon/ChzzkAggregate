from typing import List

from sqlalchemy import URL
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from domain.channel import Channel
from repositories.interface.channel_repository_interface import ChannelRepository
from repositories.sqlalchemy.base_sqlalchemy_repository import ChannelTable, Base, BaseSqlAlchemyRepository


class SqlAlchemyChannelRepository(BaseSqlAlchemyRepository[ChannelTable],
                                  ChannelRepository):
    def __init__(self, url:URL):
        super().__init__(url)

    def save_channels(self, channels: List[Channel]) -> int:
        entities = [t.model_dump() for t in channels]
        with Session(self.engine) as session:
            stmt = insert(ChannelTable).values(entities)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["channel_id"]
            )

            session.execute(stmt)
            session.commit()
            return len(entities)
