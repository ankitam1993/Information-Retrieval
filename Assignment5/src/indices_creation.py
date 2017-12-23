import json
from collections import defaultdict
from Encoding_decoding import *
from API_extract_statistics import *
import struct
from random import *
import copy
import os
from datetime import  datetime
import sys

class create_index(object):

    def __init__(self,data=None):
        self.data = data
        self.term_vectors = dict()
        self.docNo_sceneId_mapping = dict()
        self.sceneId_docNo_mapping = dict()
        self.docNo_playId_mapping = dict()
        self.docNo_length = dict()
        self.positional_index = defaultdict(list)
        self.lookup_table = dict()
        self.inverted_lists = []
        self.manifest_pointer = None

    # create docNo_sceneId_mapping , docNo_playId_mapping, sceneId_docNo_mapping and term vector dictionary -
    # which will store doc number to its data. For this project, Internal docId is taken as the sceneId mapped to an integer.

    def create_mappings(self):

        s_count = 1

        for item in self.data.values():

            for scene in item:
                self.docNo_sceneId_mapping[s_count] = scene['sceneId']
                self.sceneId_docNo_mapping[scene['sceneId']] = s_count

                self.docNo_playId_mapping[s_count] = scene['playId']

                tokenised_text = scene['text'].split()

                self.term_vectors[s_count] = tokenised_text
                self.docNo_length[s_count] = len(tokenised_text)

                s_count += 1


        # This is for when new data will come, then at which index number dictionary should be updated. If this is not saved,
        # then everytime when new data will come, whole dictionary would need to be parsed.
        # Also, used in API Retreival later on - to get the total number of documents

        self.docNo_sceneId_mapping[0] = s_count

        print 'total scenes are :' , len(self.term_vectors.values())


    # Create the postings of every token of every document present in term_vectors dictionary created before.
    # and store in positional_index dictionary.
    def create_positional_index(self):
        for doc_no in range(1,self.docNo_sceneId_mapping[0]):

            vector = self.term_vectors[doc_no]

            for index,term in enumerate(vector):

                if len(self.positional_index[term]) > 0:
                    if doc_no == self.positional_index[term][-1][0]:
                       self.positional_index[term][-1][1] += 1
                       self.positional_index[term][-1][2].append(index)
                    else:
                        self.positional_index[term].append([doc_no , 1 , [index]])
                else:
                    self.positional_index[term] = [[doc_no , 1 , [index]]]

        print 'total terms in index are' , len(self.positional_index)



    def uncompressed(self,inv_list_name):
        # It will denote the starting position of the key
        fp = open(inv_list_name, 'wb')

        pos = 0

        for key, value in self.positional_index.items():

            # It denotes the number of elements to be read from the position of that key term.
            count = 0

            # It stores the total number of times this key term is occuring in all documents.
            ctf = 0

            # It stores the total number of documents in which this key term is occuring.
            df = len(value)

            for j, val in enumerate(value):

                z = struct.pack('i', val[0])
                fp.write(z)
                count += len(z)

                z = struct.pack('i', val[1])
                fp.write(z)
                count += len(z)

                for x in val[2]:
                    z = struct.pack('i', x)
                    fp.write(z)
                    count += len(z)

                ctf += val[1]

            self.lookup_table[key] = [pos, count, df, ctf]
            pos += count

        fp.close()

    # It create lookup table, and performs Delta and V-byte encoding of numbers

    def compress_data(self,inv_list_name):

        fp = open(inv_list_name, 'wb')

        delta_encoded = Delta_encoding(self.positional_index)

        print delta_encoded['bashful']
        #print self.positional_index['foul']

        pos = 0

        for key, value in delta_encoded.items():

            # It denotes the number of elements to be read from the position of that key term.
            count = 0

            # It stores the total number of times this key term is occuring in all documents.
            ctf = 0

            # It stores the total number of documents in which this key term is occuring.
            df = len(value)

            for _, val in enumerate(value):

                temp = []

                temp.extend(encode_number(val[0]))
                temp.extend(encode_number(val[1]))

                for x in val[2]:
                    temp.extend(encode_number(x))

                for t in temp:
                    z = struct.pack('B', t)
                    fp.write(z)
                    count += len(z)

                ctf += val[1]

            self.lookup_table[key] = [pos, count, df, ctf]
            pos += count

        fp.close()

    # Calculate the shortest, longest scene and play
    def calculate_lengths_scene_play(self,manifest_name):

        average_length_scene = float(sum(self.docNo_length.values()))/len(self.docNo_length)
        shortest_scene = sorted(self.docNo_length, key=self.docNo_length.get)[0]

        longest_play_length = 0
        longest_play_id = -1

        shortest_play_length = sys.maxint
        shortest_play_id = -1
        flag = 0

        for doc,play in self.docNo_playId_mapping.items():

            if flag == 0:
                count = self.docNo_length[doc]
                id = play
                flag = 1

            else:

                if play == id:
                    count += self.docNo_length[doc]

                else:

                    if count > longest_play_length:
                        longest_play_length = count
                        longest_play_id = id

                    if count < shortest_play_length:
                        shortest_play_length = count
                        shortest_play_id = id

                    count = self.docNo_length[doc]
                    id = play

        print 'Average length of scene is :' , average_length_scene
        print 'Shortest scene is :' , self.docNo_sceneId_mapping[shortest_scene]
        print 'longest play is: {0} of length {1}'.format(longest_play_id,longest_play_length)
        print 'shortest play is: {0} of length {1}'.format(shortest_play_id, shortest_play_length)

        self.manifest_pointer = open(manifest_name,'a')

        self.manifest_pointer.write('Average length of scene is: ' + str(average_length_scene) + os.linesep)
        self.manifest_pointer.write('Shortest scene is: ' + str(self.docNo_sceneId_mapping[shortest_scene]) + os.linesep)
        self.manifest_pointer.write('longest play is: ' + str(longest_play_id) + ' of length: ' + str(longest_play_length) + os.linesep)
        self.manifest_pointer.write('shortest play is: ' + str(shortest_play_id) + ' of length: ' + str(shortest_play_length) + os.linesep)

        self.manifest_pointer.close()


    # save the files
    def save_data(self,inv_list_name,lookup_table_name, docNo_sceneId_name,
                  sceneId_docNo_name,docNo_playId_name,docNo_length_name,manifest_name):


        #print self.inverted_lists[0:20]
        print 'posting of word "orlando" :' , self.positional_index['orlando']

        with open(lookup_table_name, 'w') as fp:
            json.dump(self.lookup_table, fp)

        with open(docNo_sceneId_name, 'w') as fp:
            json.dump(self.docNo_sceneId_mapping, fp)

        with open(sceneId_docNo_name, 'w') as fp:
            json.dump(self.sceneId_docNo_mapping, fp)

        with open(docNo_playId_name, 'w') as fp:
            json.dump(self.docNo_playId_mapping, fp)

        with open(docNo_length_name, 'w') as fp:
            json.dump(self.docNo_length, fp)


        self.manifest_pointer = open(manifest_name,'w')

        self.manifest_pointer.write(inv_list_name + os.linesep)
        self.manifest_pointer.write(lookup_table_name + os.linesep)
        self.manifest_pointer.write(docNo_sceneId_name + os.linesep)
        self.manifest_pointer.write(sceneId_docNo_name + os.linesep)
        self.manifest_pointer.write(docNo_playId_name + os.linesep)
        self.manifest_pointer.write(docNo_length_name + os.linesep)

        self.manifest_pointer.close()

    # Compute the dice coefficient.
    def compute_coefficient(self,dice_coeff,token,internal_vocab,api):

        li = self.positional_index[token]

        l_token = len(li)

        max_score = -1
        max_word = None

        # Collection term frequency of the token
        n_a = api.get_ctf(token)

        # For each word in vocabulary, find score
        for en,word in enumerate(internal_vocab):

          if token != word:

            # Collection term frequency of the word
            n_b = api.get_ctf(word)

            # Get the posting list of the word
            lw = self.positional_index[word]
            l_word = len(lw)

            i,j = 0,0 # i - index for token , j - index for word

            n_ab = 0 # frequency of two words occurring together

            while(i < l_token and j < l_word):

                if li[i][0] < lw[j][0]: # d_a < d_b
                    i += 1

                elif li[i][0] > lw[j][0]: # d_a > d_b
                    j += 1

                else:     # d_a == d_b , compute the n_ab

                    assert li[i][0] == lw[j][0]

                    k,l = 0,0 # index for token, word

                    while(k < li[i][1] and l < lw[j][1]):

                        if abs(li[i][2][k] - lw[j][2][l]) != 1 and li[i][2][k] < lw[j][2][l] :
                            k += 1
                        elif abs(li[i][2][k] - lw[j][2][l]) != 1 and li[i][2][k] > lw[j][2][l]:
                            l += 1
                        elif abs(li[i][2][k] - lw[j][2][l]) == 1 and li[i][2][k] < lw[j][2][l] :
                            k += 1
                            n_ab +=1
                        elif abs(li[i][2][k] - lw[j][2][l]) == 1 and li[i][2][k] > lw[j][2][l] :
                            l += 1
                            n_ab += 1

                    i += 1
                    j += 1

            score = float(( 1.0*n_ab )) / (n_a + n_b)

            if score > max_score:
                max_score = score
                max_word = word

        dice_coeff[token] = max_word
        return dice_coeff


    # It will generate 700 random words and then find their nearest word.
    def nearest_word(self):

        f1 = open('../data/oneword_query.txt','w')
        f2 = open('../data/twoword_query.txt', 'w')

        api = API(self.lookup_table)
        vocabulary = api.get_vocabulary()

        random_tokens = []
        dice_coeff = dict()

        for _ in range(0,100):
            random_tokens.append(sample(vocabulary,7))
            [vocabulary.remove(z) for z in random_tokens[-1]]

        vocabulary = api.get_vocabulary()
        print 'length is vocab' , len(vocabulary)
        c = 0

        start = datetime.now()

        for set in random_tokens:

            f1.write(" ".join(set) + os.linesep)

            for token in set:
                dice_coeff[token] = []

                print 'computing dice coefficient of {0} - {1}'.format(c,token)

                dice_coeff = self.compute_coefficient(dice_coeff,token,vocabulary,api)

                out = token + ' ' + dice_coeff[token] + ' '
                f2.write(out)
                c += 1
                print out

            f2.write(os.linesep)

        end = datetime.now()

        print 'time for all 700 words are : ', str(end - start)

        f1.close()
        f2.close()


    def fit(self,flag,dc_flag):

        self.create_mappings()
        self.create_positional_index()

        if flag == 'comp':

            print 'saving compressed index....'
            self.compress_data('../results/compressed/comp_Inverted_list')

            self.save_data('../results/compressed/comp_Inverted_list',
                           '../results/compressed/comp_lookup_table.txt',
                           '../results/compressed/comp_docNo_sceneId.txt',
                           '../results/compressed/comp_sceneId_docNo.txt',
                           '../results/compressed/comp_docNo_playId.txt',
                           '../results/compressed/comp_docNo_length.txt',
                            '../data/comp_manifest')

            self.calculate_lengths_scene_play('../data/comp_manifest')

        else:
            print 'saving uncompressed index....'
            self.uncompressed('../results/uncompressed/unc_Inverted_list')

            self.save_data('../results/uncompressed/unc_Inverted_list',
                           '../results/uncompressed/unc_lookup_table.txt',
                           '../results/uncompressed/unc_docNo_sceneId.txt',
                           '../results/uncompressed/unc_sceneId_docNo.txt',
                           '../results/uncompressed/unc_docNo_playId.txt',
                           '../results/uncompressed/unc_docNo_length.txt',
                           '../data/unc_manifest')

            self.calculate_lengths_scene_play('../data/unc_manifest')

        if dc_flag:
            self.nearest_word()





