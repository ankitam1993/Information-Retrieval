import json
import re
from Encoding_decoding import *
from API_extract_statistics import *
from collections import defaultdict, Counter
import struct
import datetime , copy
import queue as Q

class document_score(object):

    def __init__(self,query_path,config_path):

        self.path = config_path
        self.query_path = query_path
        self.inverted_list_pointer = None
        self.lookup_table = None
        self.docNo_sceneId = None
        #self.sceneId_docNo = None
        #self.docNo_playId = None
        self.flag = 0
        self.q = Q.PriorityQueue()
        self.queries_inv_lists = []

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

        #with open(t[3]) as f2:
        #    self.sceneId_docNo = json.load(f2)

        #with open(t[4]) as f2:
        #    self.docNo_playId = json.load(f2)

    def extract_compressed_lists(self):

        f = open(self.query_path, 'r')
        api = API(self.lookup_table)

        queries = f.read().splitlines()

        j = 0
        times = []

        for query in queries:
            self.queries_inv_lists = []
            start = datetime.datetime.now()
            j += 1
            # For every word in the query, get its posting
            for word in query.split():
                self.inverted_list_pointer.seek(api.get_position(word))
                z = self.inverted_list_pointer.read(api.get_length(word))

                # Unpack is in different way for both compressed and uncompressed,
                li = struct.unpack('%dB' % len(z), z)

                # Now for compressed index, do the Delta and VByte decoding
                # print word, li
                li = VByte_decoding(li)
                li = Delta_decoding(li)

                self.queries_inv_lists.append(li)

            self.find_score()
            end = datetime.datetime.now()
            t = end - start

            print 'time taken by compressed index for query: {0} is {1}'.format(j, t)
            times.append(t)

        print 'total time(in sec) taken by compressed index is :', sum(times, datetime.timedelta()).total_seconds()
        print 'average time(in sec) taken by compressed index is :' , sum(times, datetime.timedelta()).total_seconds() / len(times)
        self.inverted_list_pointer.close()


    def extract_uncompressed_lists(self):

        f = open(self.query_path, 'r')
        api = API(self.lookup_table)

        queries = f.read().splitlines()

        j = 0
        times = []

        for query in queries:
            self.queries_inv_lists = []
            start = datetime.datetime.now()
            j += 1
            # For every word in the query, get its posting
            for word in query.split():

                self.inverted_list_pointer.seek(api.get_position(word))
                z = self.inverted_list_pointer.read(api.get_length(word))

                # Unpack is in different way for both compressed and uncompressed,
                li = struct.unpack('%di' % (len(z) / 4), z)

                self.queries_inv_lists.append(li)

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
            self.extract_compressed_lists()

        else:
            self.extract_uncompressed_lists()

    def find_score(self):

        temp_lists = copy.copy(self.queries_inv_lists)

        for doc in range(1,self.docNo_sceneId['0']):
            s_d = 0
            for i in range(0,len(temp_lists)):
                if len(temp_lists[i]) > 0 and temp_lists[i][0] == doc:
                    s_d += temp_lists[i][1]
                    temp_lists[i] = temp_lists[i][2 + temp_lists[i][1] :]

            self.q.put((-1*s_d,doc))

        return self.q

        #for _ in range(10):

        #    if not self.q.empty():
        #        print self.q.get()











