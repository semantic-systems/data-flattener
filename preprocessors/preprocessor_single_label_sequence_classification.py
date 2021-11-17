from abc import abstractmethod
from preprocessors.preprocessor_base import Preprocessor


class PreprocessorSingleLabelSequenceClassification(Preprocessor):
    @abstractmethod
    def preprocess(self):
        raise NotImplementedError
