from abc import ABC
from datetime import datetime
from typing import List

from sqlalchemy import Boolean, DateTime, Integer, String, ForeignKey, ForeignKeyConstraint, Table, Column, \
    create_engine, URL
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session
from dataclasses import dataclass

class Base(DeclarativeBase):
    ...

@dataclass
class ChannelTable(Base):
    __tablename__ = 'channel'
    channel_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    channel_name: Mapped[str] = mapped_column(String(512))
    channel_image_url: Mapped[str] = mapped_column(String(512))
    verified_mark: Mapped[bool] = mapped_column(Boolean)

    channel_snapshots: Mapped[List["ChannelSnapshotTable"]] = relationship(
        back_populates="channel",
        cascade="all, delete-orphan",
    )

class ChannelSnapshotTable(Base):
    __tablename__ = 'channel_snapshot'
    channel_id: Mapped[str] = mapped_column(String(512),
                                            ForeignKey("channel.channel_id"),
                                            primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    follower_count: Mapped[int] = mapped_column(Integer)
    channel: Mapped["ChannelTable"] = relationship(
        back_populates="channel_snapshots"
    )

class LiveSnapshotTable(Base):
    __tablename__ = 'live_snapshot'
    live_id: Mapped[str] = mapped_column(String(512), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    live_title: Mapped[str] = mapped_column(String(512))
    concurrent_user_count: Mapped[int] = mapped_column(Integer)
    open_date: Mapped[datetime] = mapped_column(DateTime)
    adult: Mapped[bool] = mapped_column(Boolean)
    tags: Mapped[List["TagTable"]] = relationship(
        secondary="live_snapshot_tag",
        cascade="save-update, merge"
    )

    category_type: Mapped[str] = mapped_column(String(512))
    live_category: Mapped[str] = mapped_column(String(512))
    live_category_value: Mapped[str] = mapped_column(String(512))
    channel_id: Mapped[str] = mapped_column(String(512),
                                            ForeignKey("channel.channel_id"))

class TagTable(Base):
    __tablename__ = "tag"
    tag_name: Mapped[str] = mapped_column(String(512), primary_key=True)

live_snapshot_tag = Table(
    "live_snapshot_tag",
    Base.metadata,
    Column("live_id", String(512), primary_key=True),
    Column("timestamp", DateTime, primary_key=True),
    Column("tag_name", ForeignKey("tag.tag_name"), primary_key=True),
    ForeignKeyConstraint(
        ["live_id", "timestamp"],
        ["live_snapshot.live_id", "live_snapshot.timestamp"],
    )
)

class BaseSqlAlchemyRepository[T](ABC):
    def __init__(self, url: URL):
        self.engine = create_engine(url)

if __name__ == "__main__":
    POSTGRES_URI = "postgresql+psycopg2://postgres:mood2301@100.94.38.27:2134"
    engine = create_engine(POSTGRES_URI, echo=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
