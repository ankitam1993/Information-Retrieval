import json
import re
from Encoding_decoding import *
from API_extract_statistics import *
from collections import defaultdict, Counter
import struct
import datetime , copy
import queue as Q
import prob_scores
import os

class probabilistic_score(object):

    def __init__(self,query_path,config_path,fname):

        self.path = config_path
        self.query_path = query_path
        self.inverted_list_pointer = None
        self.lookup_table = None
        self.docNo_sceneId = None

        self.flag = 0
        self.q = Q.PriorityQueue()
        self.queries_inv_lists = []

        # new line added - dictionary having lengths of each document
        self.docNo_length = None

        # new line - added - api for geting term statistics
        self.api = None
        self.scores_api = None

        # new line added - it will store the freq of every word in the given query
        self.query_word_freq = None

        # new line added - it will save the open file pointer
        self.f = open(fname, 'w')

        self.all_inverted_lists = None
        self.query_word_freq = None
        self.avgdl = None
        self.tot_words = None
        self.result_docs = dict()

    def extract_data(self):

        with open(self.path,'r') as f:
            t = f.read().splitlines()


        # Storing the inverted list pointer
        self.inverted_list_pointer = open(t[0],'rb')

        if re.findall('.*results/(.*)/',t[0])[0] == 'compressed':
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
        self.avgdl = float(sum(self.docNo_length.values())) / len(self.docNo_length)
        self.tot_words = sum(self.docNo_length.values())

        print 'total words are :' , self.tot_words

    def get_inverted_list(self,word):


        self.inverted_list_pointer.seek(self.api.get_position(word))
        z = self.inverted_list_pointer.read(self.api.get_length(word))

        # Unpack is in different way for both compressed and uncompressed,
        li = struct.unpack('%di' % (len(z) / 4), z)

        return li

    def get_word_freq_posting(self, query):

        temp = defaultdict(int)

        self.query_word_freq = dict()
        self.queries_inv_lists = []

        for word in query.split():
            temp[word] += 1

        for i, word in enumerate(set(query.split())):

            self.query_word_freq[i] = [word, temp[word]]
            li = self.get_inverted_list(word)
            self.queries_inv_lists.append(li)

        #print 'here', self.query_word_freq

    def extract_uncompressed_lists(self):

        f = open(self.query_path, 'r')

        queries = f.read().splitlines()
        j = 0
        times = []

        for query in queries:

            start = datetime.datetime.now()
            j += 1
            # For every word in the query, get its frequency and the posting
            self.get_word_freq_posting(query)

            self.find_score()

            end = datetime.datetime.now()
            t = end-start
            print 'time taken by uncompressed index for query: {0} is {1}'.format(j,t)
            times.append(t)

        print 'total time(in sec) taken by uncompressed index is :', sum(times, datetime.timedelta()).total_seconds()
        print 'average time(in sec) taken by uncompressed index is :', sum(times, datetime.timedelta()).total_seconds() / len(times)
        self.inverted_list_pointer.close()


    def fit(self):
        self.extract_data()

        if self.flag == 1:
            print 'Api retrieval implementation has only been done for uncompressed index'

        else:
            self.scores_api = prob_scores.scores(3)
            self.extract_uncompressed_lists()

    def get_all_documents(self):

        documents = set()

        for inverted_list in self.queries_inv_lists:

            i = 0

            while (i < len(inverted_list)):
                documents.add(inverted_list[i])  # 1 , 2, 23 , 24 , 2 , 5 , 1, 2, 3, 4 ,5 , 10
                i = i + inverted_list[i + 1] + 2

        return list(documents)

    def find_score(self):

        temp_lists = copy.copy(self.queries_inv_lists)

        all_documents = self.get_all_documents()

        all_documents.sort()

        for doc_no in all_documents:
            s_d = 0

            for i in range(0, len(temp_lists)):

                if len(temp_lists[i]) > 0 and temp_lists[i][0] == doc_no:

                    doc_length = self.docNo_length[str(doc_no)]
                    tf = temp_lists[i][1]
                    df = self.api.get_df(self.query_word_freq[i][0])
                    ctf = self.api.get_ctf(self.query_word_freq[i][0])
                    q_freq = self.query_word_freq[i][1]

                    # N, total_words , doc_length , tf , df ,ctf, qf , avgdl
                    s_d += self.scores_api.get_score(self.docNo_sceneId['0']-1,self.tot_words, doc_length , tf , df , ctf , q_freq,self.avgdl)
                    temp_lists[i] = temp_lists[i][2 + temp_lists[i][1] :]

                # For QL models, taking the score even if the doc is not present in the inverted list
                else:

                    doc_length = self.docNo_length[str(doc_no)]
                    tf = 0
                    df = 0
                    ctf = self.api.get_ctf(self.query_word_freq[i][0])
                    q_freq = self.query_word_freq[i][1]
                    s_d += self.scores_api.get_score(self.docNo_sceneId['0']-1,self.tot_words, doc_length , tf , df , ctf , q_freq,self.avgdl)

            self.q.put((-1 * s_d, doc_no))

            self.result_docs[doc_no] = s_d

    def get_resulted_docs(self):
        return self.result_docs












