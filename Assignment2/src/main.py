# This code is to generate compressed/uncompressed indexes.
# It takes two arguments - one is json data path and other if a user wants to make uncompressed data - then --uncompressed
# argument. By default, it will generate a compressed index.

import json
import argparse
#from create_index import *
from indices_creation import *
import datetime

parser = argparse.ArgumentParser()
parser.add_argument('--json_data_path', '-d', help="Json File Path",required = True)
parser.add_argument('--uncompressed', '-u', action='store_true',help="For uncompressed Inverted Lists")
parser.add_argument('--compressed', '-c', action='store_true',help="For compressed Inverted Lists")
parser.add_argument('--dcoefficient', '-dc', action='store_true',help="For Dice coefficient computation")
args = parser.parse_args()

data_path = args.json_data_path

# Load data file
with open(data_path) as json_data:
    data = json.load(json_data)


# By default compression method
if args.compressed :
    flag = 'comp'

elif args.uncompressed:
    flag = 'unc'
else:
    flag = 'comp'
    print 'no argument haas been passed : default compressed index'

start = datetime.datetime.now()

ci = create_index(data)
ci.fit(flag,args.dcoefficient)

end = datetime.datetime.now()

#print u_index.items()[0:4]

print 'time taken to create indexes and auxilliary data :',format(end-start)

