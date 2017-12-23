# Change the window size

import json
import re
from Encoding_decoding import *
from API_extract_statistics import *
from collections import defaultdict, Counter
import struct
import datetime , copy
import queue as Q
import prob_scores
import os , sys

class Model(object):

    def __init__(self,query_path,config_path,posting_lists):

        self.path = config_path
        self.query_path = query_path

        # Set as the query length dynamically, can be given as a parameter from outside
        self.window_size = None

        self.inverted_list_pointer = None
        self.lookup_table = None
        self.docNo_sceneId = None

        self.q = Q.PriorityQueue()
        self.queries_inv_lists = posting_lists

        # new line added - dictionary having lengths of each document
        self.docNo_length = None

        # new line - added - api for geting term statistics
        self.api = None
        self.scores_api = None

        # new line added - it will save the open file pointer
        self.f = open('../results/boolean_and.trecrun', 'w')

        self.tot_words = None
        self.result_docs = dict()
        self.window_posting_list = []

    def extract_data(self):

        with open(self.path,'r') as f:
            t = f.read().splitlines()


        # Storing the inverted list pointer
        self.inverted_list_pointer = open(t[0],'rb')

        if re.findall('.*results/(.*)/',t[0])[0] == 'compressed':
            print 'API Retieval for compressed file is not handled'
            sys.exit()

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
        self.tot_words = sum(self.docNo_length.values())

        print 'total words are :' , self.tot_words

    def get_inverted_list(self,word):


        self.inverted_list_pointer.seek(self.api.get_position(word))
        z = self.inverted_list_pointer.read(self.api.get_length(word))

        # Unpack is in different way for both compressed and uncompressed,
        li = struct.unpack('%di' % (len(z) / 4), z)

        return li

    def get_query_posting(self, query):

        self.queries_inv_lists = []

        for word in query.split():
            li = self.get_inverted_list(word)
            self.queries_inv_lists.append(li)

    def extract_uncompressed_lists(self):

        if os.path.exists(self.query_path):

            f = open(self.query_path, 'r')
            queries = f.read().splitlines()
        else:
            queries = [self.query_path]

        j = 0
        times = []
        args = self.scores_api.get_params()

        for query in queries:

            start = datetime.datetime.now()
            j += 1
            self.window_size = len(query.split())
            print 'processing query' , j , query
            # For every word in the query, get its frequency and the posting
            self.get_query_posting(query)

            self.find_score(query)
            self.save_results_Trec(j, args)

            end = datetime.datetime.now()
            t = end-start
            print 'time taken by uncompressed index for query: {0} is {1}'.format(j,t)
            times.append(t)

        print 'total time(in sec) taken by uncompressed index is :', sum(times, datetime.timedelta()).total_seconds()
        print 'average time(in sec) taken by uncompressed index is :', sum(times, datetime.timedelta()).total_seconds() / len(times)
        self.inverted_list_pointer.close()


    def fit(self):

        if len(self.queries_inv_lists) == 1:
            print 'invalid query'

        elif len(self.queries_inv_lists) > 1:

             self.window_size = len(self.queries_inv_lists)
             self.find_score(self.query_path)
             return self.result_docs, self.window_posting_list

        else:

            self.extract_data()
            self.scores_api = prob_scores.scores(3)
            self.extract_uncompressed_lists()
            return self.result_docs, self.window_posting_list

    def get_all_documents(self):

        documents = set()

        for inverted_list in self.queries_inv_lists:

            i = 0

            while (i < len(inverted_list)):
                documents.add(inverted_list[i])  # 1 , 2, 23 , 24 , 2 , 5 , 1, 2, 3, 4 ,5 , 10
                i = i + inverted_list[i + 1] + 2

        return list(documents)

    def find_score(self,query):

        n_words = len(self.queries_inv_lists)

        if n_words == 1:

            i = 0

            while (i < len(self.queries_inv_lists[0])):

                doc_no = self.queries_inv_lists[0][i]
                doc_length = self.docNo_length[str(doc_no)]

                tf = self.queries_inv_lists[0][i+1]
                ctf = self.api.get_ctf(query)

                s_d = self.scores_api.get_score(0, self.tot_words, doc_length, tf,0, ctf, 1, 0)

                i = i + self.queries_inv_lists[0][i + 1] + 2

                self.q.put((-1 * s_d, doc_no))
                self.result_docs[doc_no] = s_d

            self.window_posting_list = self.queries_inv_lists[0]

        else:

            temp = copy.copy(self.queries_inv_lists)

            flag = 1
            doc_tf = dict()
            ctf = 0

            while (flag == 1):

                all_docs = []

                # get the doc number of all query words
                for i in range(0,n_words):
                    if len(temp[i]) > 0:
                        all_docs.append(temp[i][0])

                    # if any one of the inverted list is ended, break
                    else:
                        flag = 0
                        break

                if flag == 1:
                    m = max(all_docs)

                    # all words are in the same document and set window size = document size
                    if len(set(all_docs)) == 1:

                        self.window_size = self.docNo_length[str(m)]
                        tf,positions = self.check_order(temp)

                        if tf > 0:
                            doc_tf[m] = tf
                            ctf += tf

                            # add document number
                            self.window_posting_list.append(m)

                            # add tf
                            self.window_posting_list.append(tf)

                            # add positions
                            self.window_posting_list.extend(positions)

                    # increment all the lists
                        for i in range(0, n_words):
                            temp[i] = temp[i][2 + temp[i][1]:]

                # if all the words are not in the same document, then increment all the lists
                # till the point when doc_number < mamximum doc number
                    else:

                        for i in range(0, n_words):
                            while temp[i][0] < m:
                                temp[i] = temp[i][2 + temp[i][1]:]


            print 'total' , ctf
            self.result_docs = doc_tf

            for doc_no in doc_tf:

                doc_length = self.docNo_length[str(doc_no)]
                tf = doc_tf[doc_no]
                s_d = self.scores_api.get_score(0,self.tot_words, doc_length , tf , 0 , ctf ,1,0)

                self.q.put((-1 * s_d, doc_no))
                self.result_docs[doc_no] = s_d

    def check_order(self,temp):

        temp2 = []
        positions = []

        # get all the indexes first
        for i in range(0, len(temp)):
            temp2.append(temp[i][2:2+temp[i][1]])

        tf = 0

        flag2 = 1

        while (flag2 == 1):

            min_index = []

            for i in range(0,len(temp2)):
                if len(temp2[i]) > 0:
                    min_index.append(temp2[i][0])

                # if any of list is ended break
                else:
                    flag2 = 0
                    break

            if flag2 == 1:
                minimum = min(min_index)
                maximum = max(min_index)

                # no duplicate indexes
                if len(set(min_index)) == len(min_index):

                    # indexes are in the window size, then inrement its frequency and the list by 1 ( double dipping not handled)
                    if maximum - minimum < self.window_size:

                        tf += 1
                        positions.append(minimum)

                        for i in range(0, len(temp2)):
                             temp2[i] = temp2[i][1:]
                    else:
                        # no duplicate indexes but not present in the window, then increment the minimum index
                        for i in range(0, len(temp2)):
                            if temp2[i][0] == minimum:
                                temp2[i] = temp2[i][1:]

            # if duplicate words are present, then increment the index of 2nd time occuring word
                else:
                    visited = []

                    for i in range(0, len(temp2)):
                        if temp2[i][0] in visited:
                            temp2[i] = temp2[i][1:]
                        else:
                            visited.append(temp2[i][0])

        return tf,positions

    def save_results_Trec(self, q_no, args):

        i = 0
        print 'saving results'
        while (not self.q.empty()):

            docno_score = self.q.get()
            self.f.write('Q' + str(q_no))
            self.f.write(' ' + 'skip')
            self.f.write(' ' + str(self.docNo_sceneId[str(docno_score[1])]))
            self.f.write(' ' + str(i))
            self.f.write(' ' + str(-1 * docno_score[0]))
            self.f.write(' ' + 'amehta-qldir')

            for k in args:
                self.f.write('-' + str(k))

            self.f.write(os.linesep)

            i += 1










