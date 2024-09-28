from typing import Any
import lightning as L
import torch
from .csl.models.model import SimilarityRecognizer
from pymilvus import connections
from pymilvus import utility
from pymilvus import CollectionSchema, FieldSchema, DataType, Collection
from loguru import logger


class CSLModule(L.LightningModule):
    def __init__(self, model_type, weight_path, collection_name='csl_test', batch_size_test=16):
        super().__init__()
        self.model = SimilarityRecognizer(model_type, batch_size_test)
        self.model.load_pretrained_weights(weight_path)
        self.collection_name = collection_name
        self.milvis_connect()
    
    
    def forward(self, frames) -> Any:
        return self.model.extract_features(frames)
    
    
    def predict_step(self, batch: tuple[torch.Tensor, str], batch_idx: int):
        frames, idx, t2, l, uid = batch
        return self(frames)
    
    
    def milvis_connect(self):
        connections.connect(
            alias="default", 
            host='localhost', 
            port='19530'
        )
        if utility.has_collection(self.collection_name):
            self.collection = Collection(
                name=self.collection_name,
            )
        else:
            vid = FieldSchema(
                name="id",
                dtype=DataType.INT64,
                is_primary=True,
                auto_id=True
            )

            name = FieldSchema(
                name="video_name",
                dtype=DataType.VARCHAR,
                max_length=200,
                default_value="Unknown"
            )


            features = FieldSchema(
                name="features",
                dtype=DataType.FLOAT_VECTOR,
                dim=768
            )

            schema = CollectionSchema(
                fields=[vid, name, features],
                description="3D-CSL video features",
                enable_dynamic_field=True
            )

            self.collection = Collection(
                name=self.collection_name,
                schema=schema,
                using='default',
                shards_num=2
            )
    
    
    def on_predict_batch_end(self, outputs: Any | None, batch: Any, batch_idx: int, dataloader_idx: int = 0) -> None:
        frames, idx, t2, lens, uids = batch
        data = [
            [uid + f'_{i}' for l, uid in zip(lens, uids) for i in range(l)],
            outputs.cpu().numpy()
        ]

        try:
            mr = self.collection.insert(data)
            if mr.err_count > 0:
                logger.error(f'Errors: {mr.err_count}')
        except Exception as e:
            logger.exception(e)