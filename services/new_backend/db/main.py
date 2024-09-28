from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session

from . import crud, models, schemas
from .database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI()


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



@app.get('/tasks/', response_model=list[schemas.Task])
def get_tasks(db: Session = Depends(get_db)):
    tasks = crud.get_tasks(db)
    return tasks
