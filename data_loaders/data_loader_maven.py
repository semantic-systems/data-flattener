import logging
from typing import List, Dict, Any, Text
from data_loaders.data_loader_base import DataLoader
from jsonlines import jsonlines

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

EVENTS = List[Dict[Text, Any]]
EVENT = Text
TRIGGER = Text
SENTENCE_ID = int
OFFSET = List[int]


class DataLoaderMAVEN(DataLoader):

    def read(self) -> dict:
        data = {"documents": {}}
        with jsonlines.open(self.path_to_data) as lines:
            for i, line in enumerate(lines):
                data["documents"][i] = line
        return data

    def transform(self, data) -> pd.DataFrame:
        doc_id = []
        doc_title = []
        sentence = []
        tokens = []
        events = []
        negatives = []
        for document in data["documents"].values():
            num_sentence = len(document["content"])
            # create events
            tmp_event = []
            tmp_trigger = []
            tmp_trigger_offset = []
            sentence_index_list = []
            events_dict = {i: {"event_type": [], "trigger_word": [], "trigger_offset": []} for i in range(num_sentence)}
            try:
                self.flatten_event_representation(document, sentence_index_list, tmp_event, tmp_trigger, tmp_trigger_offset)
                self.create_event_per_sentence(events, events_dict, num_sentence, sentence_index_list, tmp_event,
                                               tmp_trigger, tmp_trigger_offset)
                self.create_content_per_sentence(doc_id, doc_title, document, sentence, tokens)
                self.create_negative_triggers(document, negatives)
            except KeyError:
                logger.error("the dataset you selected is not compatible with the training set format for maven."
                             "Note: test set flattener is not implemented.")
                raise

        aligned_data = {"doc_id": doc_id,
                        "doc_title": doc_title,
                        "sentence": sentence,
                        "tokens": tokens,
                        "events": events
                        }
        df = pd.DataFrame.from_dict(data=aligned_data)
        df['sent_index'] = df.groupby(df.doc_id).cumcount()
        df['event_type'] = df["events"].apply(lambda x: x["event_type"] if x["event_type"] else np.NaN)
        df['trigger_word'] = df["events"].apply(lambda x: x["trigger_word"] if x["event_type"] else np.NaN)
        df['trigger_offset'] = df["events"].apply(lambda x: x["trigger_offset"] if x["event_type"] else np.NaN)
        df = df.drop(columns='events')
        df.doc_title = pd.Categorical(df.doc_title)
        df['doc_index'] = df.doc_title.cat.codes

        return df

    @staticmethod
    def create_content_per_sentence(doc_id, doc_title, document, sentence, tokens):
        for content in document["content"]:
            # create columns
            sentence.append(content["sentence"])
            tokens.append(content["tokens"])
            doc_id.append(document["id"])
            doc_title.append(document["title"])

    @staticmethod
    def create_event_per_sentence(events, events_dict, num_sentence, sentence_index_list, tmp_event, tmp_trigger,
                                  tmp_trigger_offset):
        for i_1 in range(num_sentence):
            for i_2, sentence_index in enumerate(sentence_index_list):
                if i_1 in sentence_index:
                    # extract index of event for the i-th sentence in a document
                    sent_index_of_trigger = [i_3 for i_3, s_id in enumerate(sentence_index) if s_id == i_1]
                    # append events in a list corresponding to the i-th sentence
                    for j in sent_index_of_trigger:
                        events_dict[i_1]["event_type"].append(tmp_event[i_2])
                        events_dict[i_1]["trigger_word"].append(tmp_trigger[i_2][j])
                        events_dict[i_1]["trigger_offset"].append(tmp_trigger_offset[i_2][j])
                else:
                    pass
        events.extend(list(events_dict.values()))

    @staticmethod
    def flatten_event_representation(document, sentence_index_list, tmp_event, tmp_trigger, tmp_trigger_offset):
        for event in document["events"]:
            sentence_index_list.append([mention["sent_id"] for mention in event["mention"]])
            tmp_trigger.append([mention["trigger_word"] for mention in event["mention"]])
            tmp_trigger_offset.append([mention["offset"] for mention in event["mention"]])
            tmp_event.append(event["type"])

    @staticmethod
    def create_negative_triggers(document, negatives):
        pass

    def run(self):
        data = self.read()
        self.df = self.transform(data)
        self.save()
        return self.df