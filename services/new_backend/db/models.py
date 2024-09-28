import datetime

from sqlalchemy import Column
from sqlalchemy.sql import sqltypes

from .database import Base


class Task(Base):
    __tablename__ = "tasks"

    id = Column(sqltypes.Integer, primary_key=True)
    path_or_url = Column(sqltypes.String, index=True)
    creation_date = Column(sqltypes.DateTime, default=datetime.datetime.now)


class Upload(Base):
    __tablename__ = "uploads"

    id = Column(sqltypes.Integer, primary_key=True)
    path_or_url = Column(sqltypes.String, index=True)
    upload_date = Column(sqltypes.DateTime, default=datetime.datetime.now)
    webhook_url = Column(sqltypes.String, nullable=True)
    webhook_url_request_id = Column(sqltypes.Integer, nullable=True)
    task_answer = Column(sqltypes.String, nullable=True)