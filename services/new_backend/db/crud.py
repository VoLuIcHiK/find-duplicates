import pathlib
from typing import Type, List

from sqlalchemy.orm import Session, Query

from . import models, schemas
from .schemas import Upload


def create_task(db: Session, task: schemas.TaskCreate) -> models.Task:
    db_task = models.Task(**task.model_dump())
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task


def get_task(db: Session, task_id: int) -> schemas.Task:
    q: Query = db.query(models.Task).filter(models.Task.id == task_id)
    return q.first()


def remove_task(db: Session, task_id: int) -> schemas.Task:
    q: Query = db.query(models.Task).filter(models.Task.id == task_id)
    task = q.first()
    task.delete()
    db.commit()
    return task


def get_tasks(db: Session) -> list[Type[schemas.Task]]:
    return db.query(models.Task).all()


def create_upload(db: Session, upload: schemas.UploadCreate) -> models.Upload:
    db_upload = models.Upload(**upload.model_dump())
    db.add(db_upload)
    db.commit()
    db.refresh(db_upload)
    return db_upload


def get_upload(db: Session, upload_id: int) -> schemas.Upload:
    q: Query = db.query(models.Upload).filter(models.Upload.id == upload_id)
    upload = Upload.model_validate(q.first(), from_attributes=True)
    return upload


def remove_upload(db: Session, upload_id: int) -> schemas.Upload:
    q: Query = db.query(models.Upload).filter(models.Upload.id == upload_id)
    upload = q.first()
    upload.delete()
    db.commit()
    return upload


def get_uploads(db: Session, skip: int = 0, limit: int = 100) -> list[Upload]:
    raw_uploads = db.query(models.Upload).offset(skip).limit(limit).all()
    uploads = [Upload.model_validate(x, from_attributes=True) for x in raw_uploads]
    return uploads
