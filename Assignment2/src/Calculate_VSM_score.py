import json
import re , os
from Encoding_decoding import *
from API_extract_statistics import *
import struct
import datetime , copy
import queue as Q
from VSM import *

class vsm_score(object):
    def __init__(self, query_path, config_path, fname):

        self.path = config_path
        self.query_path = query_path
        self.inverted_list_pointer = None
        self.lookup_table = None
        self.docNo_sceneId = None
        # self.sceneId_docNo = None
        # self.docNo_playId = None
        self.flag = 0
        self.q = Q.PriorityQueue()
        self.queries_inv_lists = []

        # new line added - dictionary having lengths of each document
        self.docNo_length = None

        # new line - added - api for geting term statistics
        self.api = None

        # new line added - it will store the freq of every word in the given query
        self.query_word_freq = None

        # new line added - it will save the open file pointer
        self.f = open(fname, 'w')

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


    def extract_VSMscore(self,vocabulary,all_inverted_lists):

        f = open(self.query_path, 'r')

        queries = f.read().splitlines()

        j = 0
        times = []

        # new line - added
        self.vsm = VSM(self.api,vocabulary,self.docNo_sceneId['0']-1)
        args = self.vsm.get_params()

        # Prepare the doc vectors for all the documents present
        self.vsm.get_doc_vectors(all_inverted_lists)

        for query in queries:
            self.queries_inv_lists = []

            start = datetime.datetime.now()
            j += 1

            print 'query is ', query

            # For every word in the query, get its posting
            for word in query.split():
                self.queries_inv_lists.append(all_inverted_lists[vocabulary.index(word)])

            # It will make the query vector
            self.vsm.get_query_vectors(query)
            self.calculate_VSM()

            self.save_results_Trec(j, args)

            end = datetime.datetime.now()
            t = end - start

            if self.flag == 1:
                var = 'compressed'
            else:
                var = 'uncompressed'

            print 'time taken by ' , var, ' index for query: {0} is {1}'.format(j, t)
            times.append(t)

        #print 'total time(in sec) taken is :', sum(times, datetime.timedelta()).total_seconds()
        #print 'average time(in sec) taken is :', sum(times, datetime.timedelta()).total_seconds() / len(times)

        self.inverted_list_pointer.close()

        # new line - closing the file pointer
        self.f.close()

    def fit(self):
        self.extract_data()

        vocabulary = self.api.get_vocabulary()

        if self.flag == 1:

            all_inverted_lists = self.vocab_compressed_inverted_lists(vocabulary)

        else:

            all_inverted_lists = self.vocab_uncompressed_inverted_lists(vocabulary)

        self.extract_VSMscore(vocabulary, all_inverted_lists)


    def get_all_documents(self):

        documents = set()

        for inverted_list in self.queries_inv_lists:

            i = 0

            while (i < len(inverted_list)):
                documents.add(inverted_list[i])  # 1 , 2, 23 , 24 , 2 , 5 , 1, 2, 3, 4 ,5 , 10
                i = i + inverted_list[i + 1] + 2

        return list(documents)

    def calculate_VSM(self):

        all_documents = self.get_all_documents()

        all_documents.sort()

        for doc in all_documents:
            s_d = self.vsm.get_score(doc)

            self.q.put((-1 * s_d, doc))

    def save_results_Trec(self, q_no, args):

        i = 0

        while (not self.q.empty()):

            docno_score = self.q.get()
            #print docno_score

            self.f.write('Q' + str(q_no))
            self.f.write(' ' + 'skip')
            self.f.write(' ' + str(self.docNo_sceneId[str(docno_score[1])]))
            self.f.write(' ' + str(i))
            self.f.write(' ' + str(-1 * docno_score[0]))
            self.f.write(' ' + 'amehta-VSM')

            for k in args:
                self.f.write('-' + str(k))

            self.f.write(os.linesep)

            i += 1















