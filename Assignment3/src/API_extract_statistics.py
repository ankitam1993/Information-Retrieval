# This class with extract vocabulary, collection term frequency and term frequency for given term.

class API(object):

    def __init__(self,lookup_table):
        self.lookup_table = lookup_table

    def get_vocabulary(self):
        return self.lookup_table.keys()

    def get_position(self,word):
        return self.lookup_table[word][0]

    def get_length(self,word):
        return self.lookup_table[word][1]

    def get_df(self,word):
        return self.lookup_table[word][2]

    def get_ctf(self,word):
        return self.lookup_table[word][3]

