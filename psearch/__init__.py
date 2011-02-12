"""
psearch

Prospective/persistent search in python

For example, create a storage
>>> storage = MemoryStore()

Add a sequence of query id, query tuples. Queries are represented as
a list of lists of terms (top list is AND, nested is OR). For example the 
following indexes a single query with ID 0 that is (A1 OR A2) AND (B1 OR B2):
>>> index([(0, [('A1', 'A2'), ('B1', 'B2')], {'somevalue': 42})], storage)

Create a QueryMatcher to find matching queries. Documents are just lists of 
terms.
>>> matcher = QueryMatcher(storage)
>>> doc = Document({'some field': [['A2', 'B1']]})
>>> list(matcher.matches(doc))
[0]

"""
from .pstorage import TCHStore, MemoryStore, GDBMStore
from .psearch import QueryMatcher, index
from .pdoc import Document
