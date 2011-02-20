"""
Prospective search

# Create storage for the queries:
>>> from pstorage import MemoryStore
>>> storage = MemoryStore()

# Create a sequence of (query_id, query) to index:
>>> from pquery import Query
>>> query1 = Query(0, [('A1', 'A2'), ('B1', 'B2')])
>>> query2 = Query(1, [('B2',), ('C1', 'C2')])
>>> query3 = Query(2, [('B2',)], filters=[('F3', 10, 20)])
>>> queries = [query1, query2, query3]

# call the index method to create the index
>>> index(queries, storage)

# Create a QueryMatcher to find matching queries
>>> matcher = QueryMatcher(storage)

>>> from pdoc import Document
>>> def m(*a, **kw): return list(matcher.matches(Document(*a, **kw))) 
>>> m({'fieldname': [['A1', 'B2']]})
[0]
>>> m({'fieldname': [['B2']]})
[]
>>> m({'field1': [['A2', 'B2']], 'field2': [['B3', 'C1']]})
[0, 1]

>>> m({'field1':[['X', 'B2']]}, rangefilters={'F3': [1, 15]})
[2]
"""
import tempfile, os, logging
from itertools import groupby, chain
from operator import itemgetter
import numpy as np

log = logging.getLogger("psearch")

class QueryMatcher(object):
    def __init__(self, storage):
        self.storage = storage
    
    def matches(self, document):
        """Return a sequence of queries that match the given list of tokens
        """
        terms = set(document.iterprefixedterms())
        candidates = dict(chain(*(self.storage.read_posts('R', t) for t in terms)))
        for term in terms:
            to_merge = [x for x in self.storage.read_posts('T', term) if x[0] in candidates]
            for (qid, mask) in to_merge:
                candidates[qid] &= mask
        # output results that have seen all terms and pass associated filters
        for (qid, mask) in candidates.iteritems():
            if mask != 0:
                continue
            qdata = self.storage.get_data(qid, {})
            filters = qdata.get('filters', ())
            for (field, start, end) in filters:
                field_values = document.rangefilters.get(field, ())
                if not any(start < v and (end is None or end > v) for v in field_values):
                    break
            else:
                yield qid

def index(queries, storage):
    """Generate a simple index that can be used to quickly match
    documents to queries
    
    Parameters:
        `queries`: a sequence of (query_id, term_dnf) to be indexed. Term DNF
            is a disjunctive normal form of the query terms. For example
            [[A, B], [C, D]] is (A or B) and (C or D)
        `storage`: storage back end (see pstorage module)
    """
    # term id allocation. For really large data we could move to disk
    termmap = {}
    termfreqs = []
    def _tid(term):
        tid = termmap.get(term)
        if tid is None:
            tid = len(termmap)
            termmap[term] = tid
            termfreqs.append(1)
        else:
            termfreqs[tid] += 1
        return tid

    termdata = _Buffer([('qid', np.int32), ('tid', np.int32), ('pos', np.int32)])
    qcount = qloaded = 0
    for query in queries:
        qcount += 1
        # generate QID, TID, POS and write to file
        qtpgen = ((query.query_id, _tid(term), pos) 
                for (pos, or_terms) in enumerate(query.search_terms)
                for term in or_terms)
        termdata.addseq(qtpgen)
        storage.set_data(query.query_id, query.data_dict)
        qloaded += 1
    saveddata = termdata.asarray()
    
    # partition the saved data
    btype = [('tid', np.int32), ('qid', np.int32), ('mask', np.int32)]
    rare_term_buffer = _Buffer(btype)
    term_buffer = _Buffer(btype)
    for qid, vals in groupby(saveddata, itemgetter(0)):
        # each index is position in query, each list is terms
        data = [map(itemgetter(1), qtp) for (pos, qtp) in \
                groupby(vals, itemgetter(2))]
        pos_freq = [sum(termfreqs[tid] for tid in terms) for terms in data]
        min_freq = min(pos_freq)
        min_mask = 0
        min_terms = None
        for pos, (or_terms, freq) in enumerate(zip(data, pos_freq)):
            # add to min_term index data
            if freq == min_freq and min_terms is None:
                min_terms = or_terms
                continue
            pos_bit = 1 << pos
            min_mask |= pos_bit
            mask = ~pos_bit
            term_buffer.addseq((term, qid, mask) for term in or_terms)
        rare_term_buffer.addseq((term, qid, min_mask) for term in min_terms)

    # write the final index
    termmap = dict((v, k) for (k, v) in termmap.iteritems())
    _write_terms('R', termmap, rare_term_buffer.asarray(), storage)
    _write_terms('T', termmap, term_buffer.asarray(), storage)
    log.info("loaded %s/%s queries into query index: %s unique terms, %s total",
            qloaded, qcount, len(termfreqs), len(saveddata))

_plist_dtype = [('qid', np.int32), ('mask', np.int32)]
    
def _write_terms(prefix, termmap, term_array, storage):
    term_array.sort()
    for tid, vals in groupby(term_array, itemgetter(0)):
        posts = ((qid, mask) for (tid, qid, mask) in vals)
        storage.write_posts(prefix, termmap[tid], posts)

class _Buffer(object):
    """Buffers on disk array data"""
    def __init__(self, dtype):
        self.datafile = tempfile.TemporaryFile(prefix='psearch', suffix='qdata')
        self.dtype = dtype
        self.wcount = 0

    def addseq(self, seq):
        """Add a sequence of items to the buffer"""
        arr = np.fromiter(seq, self.dtype)
        buffdata = arr.tostring()
        self.wcount += len(buffdata)
        self.datafile.write(buffdata)
    
    def asarray(self):
        self.datafile.seek(0)
        size = os.fstat(self.datafile.fileno()).st_size
        if self.wcount != size:
            raise IOError("incorrect read size")
        return np.memmap(self.datafile, self.dtype, mode='r+') \
                if size > 0 else np.array([])
