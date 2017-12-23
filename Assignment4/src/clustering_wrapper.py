import argparse
import datetime
from agglomerative_clustering import *

parser = argparse.ArgumentParser()
parser.add_argument('--manifest_file_path', '-m',required = True,help="Pass the manifest file path: which has paths of inverted list and lookup table")

args = parser.parse_args()

config_path = args.manifest_file_path

cl = Agglomerative_clustering(config_path)
cl.fit()