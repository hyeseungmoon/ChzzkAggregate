from typing import List

from sqlalchemy import URL
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from domain.channel_snapshot import ChannelSnapshot
from repositories.interface.channel_snapshot_repository_interface import ChannelSnapshotRepository
from repositories.sqlalchemy.base_sqlalchemy_repository import ChannelSnapshotTable, BaseSqlAlchemyRepository


class SqlAlchemyChannelSnapshotRepository(BaseSqlAlchemyRepository[ChannelSnapshotTable],
                                          ChannelSnapshotRepository):
    def __init__(self, url:URL):
        super().__init__(url)

    def save_channel_snapshots(self, channel_snapshots: List[ChannelSnapshot]) -> int:
        entities = [t.model_dump() for t in channel_snapshots]
        with Session(self.engine) as session:
            stmt = insert(ChannelSnapshotTable).values(entities)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["channel_id", "timestamp"]
            )
            session.execute(stmt)
            session.commit()
            return len(entities)