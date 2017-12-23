import numpy as np
from numpy import *
import copy

class VSM(object):

    def __init__(self,API,vocabulary,total_collections):

        self.vocabulary = vocabulary
        self.N = total_collections
        self.doc_vectors = None
        self.doc_tf_vectors = None
        self.idf_vectors = None
        self.doc_weights = None
        self.normalised_doc = None
        self.normalised_weights = None

        self.query_vector = None
        self.query_tf_vector = None
        self.query_idf_vector = None
        self.normalised_query = None
        self.normalised_query_weights = None

        self.api = API

    def get_params(self):

        return []

    # N is number of documents
    def get_doc_vectors(self,vocabulary_inverted_list):

        self.doc_vectors = np.zeros((self.N, len(self.vocabulary)))
        self.idf_vectors = np.zeros(len(self.vocabulary))

        for term_count, term in enumerate(self.vocabulary):
            li = vocabulary_inverted_list[term_count]

            i = 0

            # Doc no starts with 1 but np array starts with 0
            while (i < len(li)):
                self.doc_vectors[li[i] - 1][term_count] = li[i + 1]
                i = i + li[i + 1] + 2

            self.idf_vectors[term_count] = float(self.N)/self.api.get_df(term)

        tf_d = 1 + ma.log10(self.doc_vectors)
        self.doc_tf_vectors = tf_d.filled(0)

        idf_d = ma.log10(self.idf_vectors)
        self.idf_vectors = idf_d.filled(0)

        self.doc_weights = self.doc_tf_vectors * self.idf_vectors
        self.normalised_doc = np.sum(np.square(self.doc_weights),axis = 1)

        self.normalised_doc_weights = self.doc_weights / np.reshape(self.normalised_doc, (self.N,1))

    def get_query_vectors(self,query):

        self.query_vector = np.zeros(len(self.vocabulary))

        for word in query.split():
            i = self.vocabulary.index(word)

            self.query_vector[i] +=1

        tf_q = 1 + ma.log10(self.query_vector)
        self.query_tf_vector = tf_q.filled(0)


    def get_score(self,doc_no):

        self.query_weights = self.query_tf_vector * self.idf_vectors
        self.normalised_query = np.sum(np.square(self.query_weights))

        #print 'normalised query is ' , self.normalised_query
        self.normalised_query_weights = self.query_weights / self.normalised_query

        a = self.doc_weights[doc_no - 1].dot(self.query_weights)

        b = math.pow(self.normalised_doc[doc_no-1]* self.normalised_query,0.5)


        doc_score = float(a) / b

        return doc_score












