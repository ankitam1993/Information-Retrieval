import json
import re , os
from Encoding_decoding import *
from API_extract_statistics import *
import struct
import datetime , copy
from linking_and_Cosine_similarity import *

class Agglomerative_clustering(object):
    def __init__(self, config_path):

        self.path = config_path
        self.inverted_list_pointer = None
        self.lookup_table = None
        self.docNo_sceneId = None
        # self.sceneId_docNo = None
        # self.docNo_playId = None
        self.flag = 0
        # new line added - dictionary having lengths of each document
        self.docNo_length = None

        # new line - added - api for geting term statistics
        self.api = None

        # new line added - it will store the freq of every word in the given query
        self.query_word_freq = None

    def extract_data(self):

        with open(self.path, 'r') as f:
            t = f.read().splitlines()

        # Storing the inverted list pointer
        self.inverted_list_pointer = open(t[0], 'rb')

        if re.findall('.*results/(.*)/', t[0])[0] == 'compressed':
            print 'API Retieval for compressed file'
            self.flag = 1

        else:
            print 'API Retieval for uncompressed file'

        # storing the lookup table
        with open(t[1]) as f2:
            self.lookup_table = json.load(f2)

        with open(t[2]) as f2:
            self.docNo_sceneId = json.load(f2)

        with open(t[5]) as f2:
            self.docNo_length = json.load(f2)

        self.api = API(self.lookup_table)

    def vocab_compressed_inverted_lists(self, vocabulary):

        all_inverted_lists = []

        for word in vocabulary:
            self.inverted_list_pointer.seek(self.api.get_position(word))
            z = self.inverted_list_pointer.read(self.api.get_length(word))

            # Unpack is in different way for both compressed and uncompressed,
            li = struct.unpack('%dB' % len(z), z)

            # Now for compressed index, do the Delta and VByte decoding
            # print word, li
            li = VByte_decoding(li)
            li = Delta_decoding(li)

            all_inverted_lists.append(li)

        return all_inverted_lists

    def vocab_uncompressed_inverted_lists(self,vocabulary):

        all_inverted_lists = []

        for word in vocabulary:

            self.inverted_list_pointer.seek(self.api.get_position(word))
            z = self.inverted_list_pointer.read(self.api.get_length(word))
            # Unpack is in different way for both compressed and uncompressed,
            li = struct.unpack('%di' % (len(z) / 4), z)
            all_inverted_lists.append(li)

        return all_inverted_lists

    def fit(self):
        self.extract_data()

        vocabulary = self.api.get_vocabulary()

        if self.flag == 1:

            all_inverted_lists = self.vocab_compressed_inverted_lists(vocabulary)

        else:

            all_inverted_lists = self.vocab_uncompressed_inverted_lists(vocabulary)

        self.find_clusters(vocabulary, all_inverted_lists)


    def find_clusters(self,vocabulary,all_inverted_lists):

        self.cl = linking_and_scoring(self.api,vocabulary,self.docNo_sceneId['0']-1)

        # Prepare the doc vectors for all the documents present
        self.cl.get_doc_vectors(all_inverted_lists)

        self.inverted_list_pointer.close()

        thresholds = arange(0.05,1.00,0.05)
        linking_choices = ['mean']
        #linking_choices = ['min,max,average,mean']

        for f in linking_choices:

            print 'linkage:' , f

            for th in thresholds:

                print 'threshold' , th
                # cluster number - doc number mapping dictionary
                clusters = defaultdict(list)
                num_clusters = 0

                for doc in range(0,self.docNo_sceneId['0']-1):

                    least_cost = float("inf")
                    least_cluster_id = -1

                    for c in clusters.keys():

                        d = self.cl.compute_distance(doc,clusters[c],f)

                        if d < th and d < least_cost:
                            least_cost = d
                            least_cluster_id = c

                    if least_cost == float("inf"):
                        num_clusters +=1
                        clusters[num_clusters] = [doc]

                    else:
                        clusters[least_cluster_id].append(doc)


                print 'total clusters for threshold {0} are : {1} '.format(num_clusters,th)

                self.save_cluster_results(clusters, th, f, num_clusters)

    # doc numbers are starting from 0
    # cluster number starting from 1
    def save_cluster_results(self,clusters,thresh,linkage,num_clusters):

        f = open('../results/cluster-%s.out'% (str(thresh)),'w')

        print 'total clusters formed are :' , num_clusters

        for c in range(0,num_clusters):

            for d in clusters[c+1]:
                f.write(str(c+1))
                f.write(' ')
                f.write(str(d))
                f.write(os.linesep)

        f.close()














