import datetime
import json
import pathlib
import secrets
raise DeprecationWarning("This module is deprecated. Use db/schemas.py instead")
from peewee import SqliteDatabase, Model, ForeignKeyField, CharField, AutoField, DateTimeField

database_folder = pathlib.Path(__file__).parent.parent
DATABASE_FILENAME = database_folder / 'data.db'

database = SqliteDatabase(DATABASE_FILENAME)


class BaseModel(Model):
    class Meta:
        database = database


class Task(BaseModel):
    task_id = AutoField()
    file_path_or_url = CharField(max_length=256)
    creation_date = DateTimeField(default=datetime.datetime.now)

    def __init__(self, file_path_or_url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if file_path_or_url is not None:
            self.file_path_or_url = str(file_path_or_url)



def create_models():
    database.connect()
    database.create_tables([Task], safe=True)
    # database.create_tables([Upload, UploadResult], safe=True)
    database.close()


def main():
    u = Task(filepath='test.jpg')
    print(json.dumps(u))


if __name__ == '__main__':
    main()
