import lightning as L
import torch
from torch.utils.data import DataLoader

from .dataset import VideoDataset


class VideoDataModule(L.LightningDataModule):
    def __init__(self, video_batch, query_file, database_file, root_dir: str):
        super().__init__()
        self.video_batch = video_batch
        self.root_dir = root_dir
        self.database_file = database_file
        self.query_file = query_file

    
    def setup(self, stage=None):
        if stage == "predict" or stage is None:
            self.mnist_test = VideoDataset(self.query_file, self.database_file, self.root_dir)
    
    
    def collate(self, data):
        frames, idx, t2, l, uid = zip(*data)
        return torch.cat(frames), idx, t2, l, uid

    
    def predict_dataloader(self):
        return DataLoader(self.mnist_test, batch_size=self.video_batch, num_workers=12, drop_last=False, collate_fn=self.collate)