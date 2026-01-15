from typing import List

from sqlalchemy import URL, select, insert
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.orm import Session

from domain.live_snapshot import LiveSnapshot
from repositories.interface.live_snapshot_repository_interface import LiveSnapshotRepository
from repositories.sqlalchemy.base_sqlalchemy_repository import LiveSnapshotTable, \
    BaseSqlAlchemyRepository, TagTable, live_snapshot_tag


def get_or_create_tag(session: Session, name: str) -> TagTable:
    tag = session.get(TagTable, name)
    if tag is None:
        tag = TagTable(tag_name=name)
        session.add(tag)
    return tag

class SqlAlchemyLiveSnapshotRepository(BaseSqlAlchemyRepository[LiveSnapshotTable],
                                       LiveSnapshotRepository):
    def __init__(self, url:URL):
        super().__init__(url)

    def save_live_snapshots(self, live_snapshots: List[LiveSnapshot]) -> int:
        with Session(self.engine) as session:
            all_tags: set[str] = set()
            for snap in live_snapshots:
                all_tags.update(snap.tags)

            existing_tags = session.execute(
                select(TagTable.tag_name)
                .where(TagTable.tag_name.in_(all_tags))
            ).scalars().all()

            existing_tags = set(existing_tags)

            new_tags = [{"tag_name": t} for t in all_tags if t not in existing_tags]
            if new_tags:
                session.execute(insert(TagTable), new_tags)

            snapshot_rows = [
                snap.model_dump(exclude={"tags"})
                for snap in live_snapshots
            ]

            stmt = postgres_insert(LiveSnapshotTable).values(snapshot_rows)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["live_id", "timestamp"]
            )
            session.execute(stmt)

            assoc_set = set()

            for snap in live_snapshots:
                for tag_name in snap.tags:
                    assoc_set.add((
                        snap.live_id,
                        snap.timestamp,
                        tag_name
                    ))

            assoc_rows = [
                {
                    "live_id": live_id,
                    "timestamp": timestamp,
                    "tag_name": tag_name
                }
                for live_id, timestamp, tag_name in assoc_set
            ]

            if assoc_rows:
                session.execute(insert(live_snapshot_tag), assoc_rows)

            session.commit()
            return len(live_snapshots)