"""
PSearch Query

Container object for queries 
"""

class Query(object):
    def __init__(self, query_id, search_terms, **data_dict):
        """Create a new Query object

        Parameters:
            `query_id`: an integer query id that unuqiely idenfies this query
            `search_terms': A disjunctive normal form of the query terms. For example
                            [[A, B], [C, D]] is (A or B) and (C or D)
            `data_dict`: key-value pairs to be stored along with the query.
        """
        self.query_id = query_id
        self.search_terms = search_terms
        self.data_dict = data_dict

    def __str__(self):
        return "Query(%s, %s, %s)" % (self.query_id, self.search_terms, self.data_dict)
