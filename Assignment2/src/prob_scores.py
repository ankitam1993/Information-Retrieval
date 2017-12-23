import math
import random

class scores(object):
    def __init__(self,s_flag):

        self.k1 = 1.2
        self.k2 = 100#random.randint(0, 1001)
        self.b = 0.75
        self.lamda = 0.1
        self.mu = 2000.0
        self.s_flag = s_flag

    def get_params(self):

        if self.s_flag == 1:
            return [self.k1 , self.k2 , self.b]

        elif self.s_flag == 2:
            return [self.lamda]

        elif self.s_flag == 3:
            return [self.mu]

    def get_score(self,N, tot_words, doc_length , tf , df ,ctf, qf , avgdl):

        # BM25 score for flag = 1
        if self.s_flag == 1:
            return self.get_bm25(N, doc_length , tf , df ,qf , avgdl)

        # QL-JM
        elif self.s_flag == 2:
             return self.get_qljm(tot_words, doc_length, tf, qf, ctf)

        # QL-DIR
        elif self.s_flag == 3:
            return self.get_qldir(tot_words, doc_length, tf, qf, ctf)


    def get_bm25(self,N, doc_length , tf , df ,qf , avgdl):

        K = self.k1 * ((1.0 - self.b) + self.b * (doc_length / avgdl))

        a = math.log10((N - df + 0.5) / (df + 0.5))

        b = (self.k1 + 1.0) * tf / (K + tf)

        c = (self.k2 + 1.0) * qf / (self.k2 + qf)

        doc_score = a * b * c

        return doc_score

    def get_qljm(self,tot_words, doc_length, tf, qf, ctf):

        #print doc_length , tf
        a = (1.0 - self.lamda) * (float(tf) / doc_length)
        b = self.lamda * (float(ctf) / tot_words)
        #print ctf , tot_words
        #print a , b
        doc_score = qf*(math.log10(a+b))

        #print doc_score

        return doc_score


    def get_qldir(self,tot_words, doc_length, tf, qf, ctf):

        #a = tf + self.mu * (ctf / tot_words)
        den = self.mu + doc_length
        a = tf/den
        b = self.mu * (float(ctf) / tot_words)/den
        #print a, b
        doc_score = qf*math.log10(a+b)

        return doc_score