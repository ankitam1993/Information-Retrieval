import argparse
import datetime
import imp

parser = argparse.ArgumentParser()
parser.add_argument('--query_path', '-q',required = True,help="Pass the Query path")
parser.add_argument('--manifest_file_path', '-m',required = True,help="Pass the manifest file path: which has paths of inverted list and lookup table")
parser.add_argument('--prox_exp', '-pe', help="provide the proximity_exp: term | ordered_window | unordered_window | boolean_and", required=True)

args = parser.parse_args()

query_path = args.query_path
config_path = args.manifest_file_path
pe = args.prox_exp

model_source = imp.load_source(pe, './%s.py' % (pe))
model = model_source.Model(query_path,config_path,[])

model.fit()
