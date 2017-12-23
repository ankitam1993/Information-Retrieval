from belief_operators import *
import os
import argparse

class QueryNode(object):
    def __init__(self):
        self.type = None
        self.children = None
        self.value = None

def save_results_Trec(q_no,value,results):

        with open('../results/uncompressed/unc_docNo_sceneId.txt') as f2:
            docNo_sceneId = json.load(f2)

        f = open('../results/' + value +'.trecrun','w')

        i = 0
        print 'saving results'

        all_sorted_docs = sorted(results, key=results.get, reverse=True)

        print len(all_sorted_docs)
        # results and docNo_sceneId  have keys 1 , 2, ... tot_docs
        for w in all_sorted_docs:

            f.write('Q' + str(q_no))
            f.write(' ' + 'skip')
            f.write(' ' + str(docNo_sceneId[str(w)]))
            f.write(' ' + str(i))
            f.write(' ' + str(results[w]))
            f.write(' ' + 'amehta')
            f.write(os.linesep)
            i += 1

        f.close()

parser = argparse.ArgumentParser()
parser.add_argument('--prior', '-p',required = True,help="Pass the prior feature: uniform / random")

args = parser.parse_args()

prior = args.prior

q = QueryNode()
q.type = 'belief_op'
q.value = 'AND'

c1 = QueryNode()
c1.type = 'prior'
c1.children = ''
c1.value = prior

c2 = QueryNode()
c2.type = 'term(s)'
c2.value = ''

c21 = QueryNode()
c21.type = 'term(s)'
c21.value = 'the'
c21.children = ''

c22 = QueryNode()
c22.type = 'term(s)'
c22.value = 'king'
c22.children = ''

c23 = QueryNode()
c23.type = 'term(s)'
c23.value = 'queen'
c23.children = ''

c24 = QueryNode()
c24.type = 'term(s)'
c24.value = 'royalty'
c24.children = ''

c2.children = [c21,c22,c23,c24]

q.children = [c1,c2]

bop = Belief_Operators()
scores = bop.get_node_scores(q)

print len(scores.keys())
save_results_Trec(1,c1.value,scores)


