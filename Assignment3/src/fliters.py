import imp
from belief_operators import *

class filters(object):

    # here first_argument must be any of : term | ordered_window | unordered_window | boolean_and
    def __init(self,first_argument,query,second_argument):

        self.proximity_exp = [first_argument,query]

        self.operator = second_argument

        self.filtered_docs = []

        self.operator_scores = []

    def get_filtered_docs(self):

        model_source = imp.load_source(self.proximity_exp[0], './%s.py' % (self.proximity_exp[0]))
        model = model_source.Model(self.proximity_exp[1], '../data/unc_manifest')

        model.fit()
        self.filtered_docs = model.get_resulted_docs()

    def get_operator_scores(self):

        self.operator_scores = belief_operators(self.operator)

    def filter_require(self):

        filter_require_doc = dict()

        for doc in self.filtered_docs.keys():

            if doc in self.operator_scores.keys():
                filter_require_doc[doc] = self.operator_scores[doc]

        return filter_require_doc


    def filter_reject(self):

        filter_reject_doc = dict()

        for doc,score in self.operator_scores.items():

            if doc not in self.filtered_docs.keys():
                filter_reject_doc[doc] = self.operator_scores[doc]

        return filter_reject_doc


