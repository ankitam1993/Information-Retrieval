import numpy as np
from numpy import *
import os

class linking_and_scoring(object):

    def __init__(self,API,vocabulary,total_collections):

        self.vocabulary = vocabulary
        self.N = total_collections
        self.doc_vectors = None
        self.api = API

    # N is number of documents, # this code has been taken from VSM Retreival model implementation
    def get_doc_vectors(self,vocabulary_inverted_list):

        print 'total documents are :' , self.N
        print 'total vocab length is' , len(self.vocabulary)

        self.doc_vectors = np.zeros((self.N, len(self.vocabulary)))
        self.idf_vectors = np.zeros(len(self.vocabulary))

        for term_count, term in enumerate(self.vocabulary):
            li = vocabulary_inverted_list[term_count]
            i = 0

            #print term_count , term
            # Doc no starts with 1 but np array starts with 0
            while (i < len(li)):

                #print term_count , term
                self.doc_vectors[li[i] - 1][term_count] = li[i + 1]
                i = i + li[i + 1] + 2

            self.idf_vectors[term_count] = float(self.N)/self.api.get_df(term)

        tf_d = 1 + ma.log10(self.doc_vectors)
        self.doc_tf_vectors = tf_d.filled(0)

        idf_d = ma.log10(self.idf_vectors)
        self.idf_vectors = idf_d.filled(0)

        self.doc_weights = self.doc_tf_vectors * self.idf_vectors

        self.normalised_doc = np.sqrt(np.sum(np.square(self.doc_weights),axis = 1))

        self.normalised_doc_weights = self.doc_weights / np.reshape(self.normalised_doc, (self.N,1))

        self.doc_vectors = self.normalised_doc_weights

    def compute_distance(self,doc,cluster,linkage):

        return eval('self.'+linkage + '(doc,cluster)')

    def min(self,doc,cluster):

        d1 = self.doc_vectors[doc]

        min_dist = float('inf')

        for d in cluster:

            d2 = self.doc_vectors[d]
            dist = 1 - self.cosine_similarity2(d1, d2)

            if dist < min_dist:
                min_dist = dist

        return min_dist

    def max(self, doc, cluster):

        d1 = self.doc_vectors[doc]

        max_dist = float('-inf')
        for d in cluster:

            d2 = self.doc_vectors[d]
            dist = 1 - self.cosine_similarity2(d1, d2)

            if dist > max_dist:
                max_dist = dist

        return max_dist

    def average(self, doc, cluster):

        d1 = self.doc_vectors[doc]

        distances = []

        for d in cluster:

            d2 = self.doc_vectors[d]
            dist = 1 - self.cosine_similarity2(d1, d2)

            distances.append(dist)

        return sum(distances)/float(len(distances))

    def mean(self, doc, cluster):

        d1 = self.doc_vectors[doc]

        cluster_vectors = []

        for c in cluster:
            cluster_vectors.append(self.doc_vectors[c])

        centroid = np.sum(cluster_vectors, axis=0)/len(cluster_vectors)

        dist = 1-self.cosine_similarity2(d1,centroid)

        return dist

    # this code has been taken from VSM Retreival model implementation
    def cosine_similarity2(self,d1,d2):

        normalised_d1 = np.sum(np.square(d1))
        normalised_d2 = np.sum(np.square(d2))

        a = d1.dot(d2)

        b = math.pow(normalised_d1 * normalised_d2, 0.5)

        doc_score = float(a) / b

        return doc_score








