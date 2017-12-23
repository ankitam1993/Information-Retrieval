import argparse
import datetime
from Calculate_doc_score import *

parser = argparse.ArgumentParser()
parser.add_argument('--query_path', '-q',required = True,help="Pass the Query path")
parser.add_argument('--manifest_file_path', '-m',required = True,help="Pass the manifest file path: which has paths of inverted list and lookup table")

args = parser.parse_args()

query_path = args.query_path
config_path = args.manifest_file_path

ds = document_score(query_path,config_path)

ds.fit()