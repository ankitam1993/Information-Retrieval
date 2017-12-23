import json
import random

with open('../results/uncompressed/unc_docNo_sceneId.txt') as f:
    docNo_sceneId = json.load(f)

# In docNo_sceneID, 0th index has total number of documents + 1 ( i.e. docNo for the next document)
Total_docs = docNo_sceneId['0'] - 1

# Uniform prob is = 1/total_docs present in the index
v = 1.0/Total_docs

uniform_data = {docNo_sceneId[str(k)]:v for k in range(1,Total_docs+1)}

random_data = {docNo_sceneId[str(k)]:random.random() for k in range(1,Total_docs+1)}

# dumping the uniform prior probabilities as a dictionary, because in belief operators scores are being passed as a
# list of documents and their scores
with open('../results/uniform.prior','w') as fu:
    json.dump(uniform_data,fu)

print 'uniform prior dumped'

with open('../results/random.prior','w') as fr:
    json.dump(random_data,fr)

print 'random prior dumped'