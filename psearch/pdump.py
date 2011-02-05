"""
pdump

provides routines to recreate queries from an index. Also a main method to 
dump an index to aid debugging.
"""
import sys
from itertools import chain
from collections import defaultdict

def first_zero(bits):
    """index of first zero bit
    
    >>> first_zero(3)
    2
    >>> first_zero(1)
    1
    >>> first_zero(11)
    2
    """
    index = 0
    while bits & 1:
        bits >>= 1
        index += 1
    return index

def recreate_queries(storage):
    queries = defaultdict(list)
    for (ptype, key, values) in storage.iteritems():
        for (query_id, mask) in values:
            position = first_zero(mask)
            query = queries[query_id]
            if len(query) <= position:
                query += [[] for _ in xrange(position - len(query) + 1)]
            query[position].append(key)
    for key in sorted(queries.iterkeys()):
        query = queries[key]
        for or_terms in query:
            or_terms.sort()
        yield key, queries[key]

def dump(storage, outfile):
    for qid, query in recreate_queries(storage):
        print >> outfile, "%s: %s" % (qid, query)

def main():
    if len(sys.argv) != 2:
        sys.exit("usage %s file" % sys.argv[0])
    dump(sys.argv[1], sys.stdout)

if __name__ == '__main__':
    main()
