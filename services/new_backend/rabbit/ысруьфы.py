import pydantic


class NNInput(pydantic.BaseModel):
    task_id: str
    task: str = 'all'
    file_path: str

    @classmethod
    def from_dict(cls, d: dict) -> 'NNInput':
        fixed_dict = {
            'task_id': str(d['task_id']),
            'task': str(d.get('task', 'all')),
            'file_path': str(d['file_path']),
        }
        return NNInput.model_validate(fixed_dict)


class NNOutput(pydantic.BaseModel):
    class ResultDict(pydantic.BaseModel):
        familiar_videos: list[str]
        description: str

        @classmethod
        def from_dict(cls, d: dict) -> 'NNOutput.ResultDict':
            fixed_dict = {
                'familiar_videos': [str(x) for x in d['familiar_videos']],
                'description': str(d['description']),
            }
            fixed_model = cls.model_validate(fixed_dict)
            return fixed_model

    inputs: NNInput
    result: ResultDict

    @classmethod
    def from_dict(cls, d: dict) -> 'NNOutput':
        fixed_dict = {
            'inputs': NNInput.from_dict(d['inputs']),
            'result': cls.ResultDict.from_dict(d['result']),
        }
        fixed_model = cls.model_validate(fixed_dict)
        return fixed_model


