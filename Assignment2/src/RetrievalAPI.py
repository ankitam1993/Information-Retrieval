import argparse
import datetime
from Calculate_probabilistic_score import *
from Calculate_VSM_score import *

parser = argparse.ArgumentParser()
parser.add_argument('--query_path', '-q',required = True,help="Pass the Query path")
parser.add_argument('--manifest_file_path', '-m',required = True,help="Pass the manifest file path: which has paths of inverted list and lookup table")
parser.add_argument('--model_No','-no',required = True,help="1 for BM25 ,2 for QL using Jelinik-Mercer smoothening ,3 for QL using Dirichlet smoothening,4 for VSM ")

args = parser.parse_args()

query_path = args.query_path
config_path = args.manifest_file_path

bm25_fname = '../results/bm25.trecrun'
vsm_fname = '../results/vsm.trecrun'
ql_jm_fname = '../results/ql_jm.trecrun'
ql_dir_fname = '../results/ql_dir.trecrun'

if args.model_No == '1' :

    print 'BM25 Implementation'
    bm_ds = probabilistic_score(query_path,config_path,bm25_fname)
    bm_ds.fit(1)

elif args.model_No == '2':
    print 'ql jm Implementation'
    ql_jm_ds = probabilistic_score(query_path, config_path, ql_jm_fname)
    ql_jm_ds.fit(2)

elif args.model_No == '3':
    print 'ql dir Implementation'
    ql_dir_ds = probabilistic_score(query_path, config_path, ql_dir_fname)
    ql_dir_ds.fit(3)

elif args.model_No == '4':
    print 'vsm Implementation'
    vsm_ds = vsm_score(query_path, config_path, vsm_fname)
    vsm_ds.fit()