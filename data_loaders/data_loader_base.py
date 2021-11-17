from pathlib import Path
from abc import abstractmethod
import pandas as pd


class DataLoader(object):

    def __init__(self, path_to_data: Path, output_path: Path):
        self.path_to_data = path_to_data
        self.output_path = output_path
        self.df = pd.DataFrame()

    @abstractmethod
    def read(self):
        raise NotImplementedError

    @abstractmethod
    def transform(self, data):
        raise NotImplementedError

    def save(self):
        self.df.to_csv(self.output_path.absolute(), sep=",", index=True, header=True)

    def run(self):
        data = self.read()
        self.df = self.transform(data)
        self.save()
        return self.df
