"""
PSearch Query

Container object for queries 
"""

class Query(object):
    def __init__(self, query_id, search_terms, **data_dict):
        self.query_id = query_id
        self.search_terms = search_terms
        self.data_dict = data_dict

    def __str__(self):
        return "Query(%s, %s, %s)" % (self.query_id, self.search_terms, self.data_dict)
