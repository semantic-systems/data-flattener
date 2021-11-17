import sys
import logging
from parsers import data_loader_parser
from data_loaders import DataLoaderMAVEN
from pathlib import Path

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    args = data_loader_parser()
    if args.data == "maven":
        data_loader_class = DataLoaderMAVEN
    else:
        raise NotImplementedError("Please select a data that you wish you flatten.")
    loader = data_loader_class(Path(args.input), Path(args.output))
    data = loader.run()
    logger.info(f"{args.data} is flattened into a dataframe.")
