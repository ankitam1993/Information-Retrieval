import math
import imp
import json

# multiple dispatch has been done while implementing this.
# lets assume that the input query node has the structure with nodes : type, children , value
# value will save type of operator when query node is of type :belief_operator and query if query node is of type: term
class Belief_Operators(object):

    # We can recursively give the score to the parent node, by traversing over all its child nodes. The child node can be
    # a belief_operator / ordered_window /unordered_window / term(s). The traversal will go till the child node is not the
    # single term
    def get_node_scores(self,parent_node):

        if parent_node.type == 'term(s)':

            all_term_docs = []

            for child in parent_node.children:
                print 'inside terms'

                model_source = imp.load_source('term','term.py')
                model = model_source.probabilistic_score(child.value, '../data/unc_manifest')

                model.fit()
                resulted_docs = model.get_resulted_docs()

                all_term_docs.append(resulted_docs)

            return all_term_docs,[]

        elif parent_node.type == 'belief_op':

            all_term_docs = []

            for child in parent_node.children:

                child_docs,p_list = self.get_node_scores(child)
                all_term_docs.extend(child_docs)

            func = 'belief_'+ parent_node.value

            resulted_docs = eval('self.' + func+'(all_term_docs)')

            return resulted_docs


        elif parent_node.type == 'ordered_window':

            model_source = imp.load_source('ordered_window.py')

            posting_lists = []
            query= ''

            for child in parent_node.children:

                if child.type == 'ordered_window':

                    resulted_docs,posting_list = self.get_node_scores(child)

                    posting_lists.append[posting_list]

                elif child.type == 'unordered_window':

                    resulted_docs,posting_list = self.get_node_scores(child)

                    posting_lists.append[posting_list]

                elif child.type == 'term(s)':

                    query += child.value

            model = model_source.Model(query, '../data/unc_manifest',posting_lists)

            model.fit()

            resulted_docs,posting_list = model.get_resulted_docs()

            return [resulted_docs],posting_list

        elif parent_node.type == 'unordered_window':

            model_source = imp.load_source('unordered_window.py')

            posting_lists = []
            query = ''

            for child in parent_node.children:

                if child.type == 'ordered_window':

                    resulted_docs, posting_list = self.get_node_scores(child)

                    posting_lists.append[posting_list]

                elif child.type == 'unordered_window':

                    resulted_docs, posting_list = self.get_node_scores(child)

                    posting_lists.append[posting_list]

                elif child.type == 'term(s)':

                    query += child.value

            model = model_source.Model(query, '../data/unc_manifest', posting_lists)

            model.fit()

            resulted_docs, posting_list = model.get_resulted_docs()

            return [resulted_docs], posting_list

        elif parent_node.type == 'prior':

            print 'inside prior'

            prior_name = parent_node.value

            with open ('../results/' + prior_name + '.prior','r') as f:
                all_docs_prior = json.load(f)

            # this will convert the doc_score in the required format : doc_no : score
            all_docs_prior = self.convert(all_docs_prior)

            return [all_docs_prior],[]


    def convert(self,all_docs_prior):

        with open('../results/uncompressed/unc_sceneId_docNo.txt') as f2:
            sceneId_docNo = json.load(f2)

        resulted_format = dict()
        a = []
        for key,val in all_docs_prior.items():
            id = int(sceneId_docNo[key])
            a.append(id)
            resulted_format[id] = val

        return resulted_format

    def get_all_documents(self,list_doc_scores):

        all_documents = []

        for doc_score in list_doc_scores:
            all_documents.extend(doc_score.keys())

        return list(set(all_documents))


    # pass the scores of all documents
    def belief_NOT(self,dic_scores):

        not_dic_scores = dict()

        for doc,score in dic_scores:
            not_dic_scores = math.log10(1 - math.pow(10,score))

        return not_dic_scores

    def belief_OR(self,list_dic_scores):

        all_documents = self.get_all_documents(list_dic_scores)

        OR_dic_scores = dict()

        for doc in all_documents:

            count = 0

            for dic_scores in list_dic_scores:
                if doc in dic_scores.keys():
                    p = dic_scores[doc]
                    count += math.log10(1-math.pow(10,p))

            OR_dic_scores[doc] = 1 - count

        return OR_dic_scores

    def belief_AND(self,list_dic_scores):

        #print list_dic_scores

        all_documents = self.get_all_documents(list_dic_scores)

        print 'total_documents' , len(all_documents)

        AND_dic_scores = dict()

        for i , doc in enumerate(all_documents):

            count = 0

            for dic_scores in list_dic_scores:
                if doc in dic_scores.keys():
                    count += dic_scores[doc]

            #print i , doc
            AND_dic_scores[doc] = count

        return AND_dic_scores

    def belief_WAND(self,list_dic_scores,LIST_WEIGHTS):

        WAND_dic_scores = dict()

        all_documents = self.get_all_documents(list_dic_scores)

        for doc in all_documents:

            count = 0

            for i,dic_scores in enumerate(list_dic_scores):
                if doc in dic_scores.keys():
                    count += LIST_WEIGHTS[i]*dic_scores[doc]

            WAND_dic_scores[doc] = count

        return WAND_dic_scores

    def belief_MAX(self,list_dic_scores):

        MAX_dic_scores = dict()

        all_documents = self.get_all_documents(list_dic_scores)

        for doc in all_documents:

            count = []

            for dic_scores in list_dic_scores:
                if doc in dic_scores.keys():
                    count.append(dic_scores[doc])

            MAX_dic_scores[doc] = max(count)

        return MAX_dic_scores

    def belief_SUM(self,list_dic_scores):

        SUM_dic_scores = dict()

        all_documents = self.get_all_documents(list_dic_scores)

        for doc in all_documents:

            count = 0

            for dic_scores in list_dic_scores:
                if doc in dic_scores.keys():
                    count += dic_scores[doc]

            SUM_dic_scores[doc] = float(count)/len(list_dic_scores)

        return SUM_dic_scores

    def belief_WSUM(self,list_dic_scores,LIST_WEIGHTS):

        WSUM_dic_scores = dict()

        all_documents = self.get_all_documents(list_dic_scores)

        SUM_WEIGHTS = 0

        for wt in LIST_WEIGHTS:
            SUM_WEIGHTS += wt

        for doc in all_documents:

            count = 0

            for i,dic_scores in enumerate(list_dic_scores):
                if doc in dic_scores.keys():
                    count += LIST_WEIGHTS[i]*dic_scores[doc]

            WSUM_dic_scores[doc] = float(count) / SUM_WEIGHTS

        return WSUM_dic_scores