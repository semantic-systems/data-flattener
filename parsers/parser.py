import argparse


def data_loader_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data", choices=['maven'], help="Name of the data to transform.")
    parser.add_argument("-i", "--input", help="Path to input data to transform.")
    parser.add_argument("-o", "--output", help="Path to output data to transform.")
    args = parser.parse_args()
    if not (args.input or args.output):
        raise ValueError("Please use -i and -o flag to assign input data path and output directory.")
    return args