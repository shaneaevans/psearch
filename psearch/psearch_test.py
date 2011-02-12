"""
Testing for psearch
"""
import sys, time, os
from itertools import chain, izip
import numpy as np

from .psearch import index, QueryMatcher
from .pdump import recreate_queries
from .pstorage import (GDBMStore, MemoryStore,
    TCHStore)
from .pdoc import Document

# storage classes to test
def get_storage_classes():
    try:
        import pytc
    except ImportError:
        import warnings
        warnings.warn("pytc module not found, disabling TCHStore tests")
        return (MemoryStore, GDBMStore)
    return (MemoryStore, GDBMStore, TCHStore)

class ReferenceSearch(object):
    """Reference implementation of search process for testing"""
    def __init__(self, queries):
        self.queries = queries

    def matches(self, document):
        ts = set(document.iterprefixedterms())
        for qid, query, data in self.queries:
            if all(any(t in ts for t in or_terms) for or_terms in query):
                yield qid

def genterms(count, nterms):
    """Generate count random term from a vocabulary of size nterms using 
    a normal distribution
    """
    spread = nterms / 2.0
    return [str(int(np.random.normal(nterms, spread)) % nterms)  \
            for _ in xrange(count)]

def gen_query(nterms):
    qlen = int(np.random.exponential(2.0)) + 1
    return [genterms(int(np.random.exponential(3.0)) + 1, nterms) \
            for _ in range(qlen)]

def gen_doc(nterms):
    doclen = int(np.random.exponential(20.0)) + 1
    fields = int(np.random.exponential(1.0)) + 1
    field_terms = []
    for field_i in xrange(fields):
        # pdoc.Document expects terms to be a list of lists
        field_terms.append(("field_%s" % field_i, [genterms(doclen, nterms)]))
    return Document(dict(field_terms))

def comparative_test(ndocs=100, nqueries=100, nterms=500):
    def ma(seq, key=None):
        return list(sorted(seq, key=key))
    queries = [(i, gen_query(nterms), {}) for i in xrange(nqueries)]
    reference = ReferenceSearch(queries)
    storage = MemoryStore()
    index(queries, storage)
    pmatcher = QueryMatcher(storage)
    for _ in xrange(ndocs):
        doc = gen_doc(nterms)
        refmatch = ma(reference.matches(doc))
        pmatch = ma(pmatcher.matches(doc))
        if pmatch != refmatch:
            differ = ma(set(pmatch) ^ set(refmatch))
            print >> sys.stderr,  """
Results differ
==============
reference: %s
psearch:   %s
document:  %s
differing queries:""" % (refmatch, pmatch, ma(doc, int))
            for qid in differ:
                print "    %r" % (queries[qid],)
            assert False, "results differ"
    print >> sys.stderr,  "identical results for %d docs" % ndocs
                
def test_indexstructure(nqueries=100, nterms=500):
    def cleandb():
        try:
            os.remove('ptest.db')
        except OSError:
            pass
    cleandb()
    for storage_class in get_storage_classes():
        storage = storage_class('ptest.db')
        queries = [(i, gen_query(nterms)) for i in xrange(nqueries)]
        qdata = ((i, q, {'query': q}) for (i, q) in queries)
        index(qdata, storage)
        # close and re-open in read mode to test the backing storage
        storage.close()
        storage = storage_class('ptest.db', True)
        matcher = QueryMatcher(storage)
        # recreate the original queries from the index and assert they are the same
        recreated_queries = recreate_queries(storage)
        for ((oid, oqueries), (rid, rqueries)) in izip(queries, recreated_queries):
            assert oid == rid, "queries out of order, or missing query id"
            squeries = storage.get_data(oid)['query']
            map(list.sort, oqueries)
            map(list.sort, rqueries)
            assert oqueries == rqueries, "recreated queries differ: %s - %s" % \
                    (oqueries, rqueries)
            map(list.sort, squeries)
            assert oqueries == squeries, "stored queries differ: %s - %s" % \
                    (oqueries, squeries)
        used_terms = set(chain(*chain(*(q for (i, q) in queries))))
        matches = matcher.matches(Document({'anyfield': [list(used_terms)]}))
        assert all(x == y for (x, y) in izip(matches, xrange(nqueries))), \
                "every query should match all terms and be returned in order"
        print >> sys.stderr,  "index data intact with %s" % storage_class.__name__
        cleandb()

def runperf(ndocs=100000, nqueries=10000, nterms=50000):
    """Timed performance test"""
    istart = time.time()
    #storage = GDBMStore('ptest.db')
    #storage = MemoryStore('ptest.db')
    storage = TCHStore('ptest.db')
    queries = [(i, gen_query(nterms), {}) for i in xrange(nqueries)]
    index(queries, storage)
    print "indexed %d queries in %f seconds" % (nqueries, time.time() - istart)
    pmatcher = QueryMatcher(storage)
    sstart = time.time()
    for _ in xrange(ndocs):
        doc = gen_doc(nterms)
        pmatch = pmatcher.matches(doc)
    total = time.time() - sstart
    docs_sec = ndocs / total
    print "%d documents processed in %f seconds (%f docs/sec)" % (ndocs,  total, docs_sec)

def main():
    if len(sys.argv) == 2 and sys.argv[1] == '-p':
        runperf()
    else:
        # these are called by nosetests, when run from the command line we 
        # can set larger defaults
        test_indexstructure(1000, 5000)
        comparative_test(1000, 2000, 10000)

if __name__ == '__main__':
    main()
