import pandas as pd
from abc import abstractmethod


class Preprocessor(object):
    def __init__(self, df: pd.DataFrame):
        self.df = df

    @abstractmethod
    def preprocess(self):
        raise NotImplementedError
