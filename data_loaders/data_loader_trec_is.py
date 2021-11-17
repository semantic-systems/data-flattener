import pandas as pd
import xml.etree.ElementTree as ET
from data_loaders.data_loader_base import DataLoader
from lxml import etree


class DataLoaderTRECIS(DataLoader):

    def read(self):
        with open(self.path_to_data) as f:
            xml = f.read()
        xml = "<root>" + xml + "</root>"
        parser = etree.XMLParser(recover=True)
        tree = etree.fromstring(xml, parser=parser)
        return tree

    def transform(self, data):
        cols = ["num", "dataset", "title", "label", "url", "sentence"]
        rows = []

        root = data.xpath('//root')
        for e in root:
            element = e.xpath('//top')
            for i in element:
                num = i.find("num").text
                dataset = i.find("dataset").text
                title = i.find("title").text
                label = i.find("type").text
                url = i.find("url").text
                sentence = i.find("narr").text

                rows.append({"num": num,
                             "dataset": dataset,
                             "title": title,
                             "label": label,
                             "url": url,
                             "sentence": sentence})
        df = pd.DataFrame(rows, columns=cols)
        return df
