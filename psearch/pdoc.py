"""
pdoc

Document class
"""
from itertools import chain
from collections import defaultdict

class Document(object):
    """Progressive search document

    This contains the following datastructures:
        textsearchterms: all the fields participating in text search and their
            terms. This contains sequences of terms, the terms in each 
            sequence are in the same order as the input document. For example:
            {'a field': [['first', 'values'], ['second values']]}
        rangefilters: a dict of fields and values that may be used as range filters.
            {'price', [100.0, 200.0]}
    each is a dictionary mapping from field name to a list of values.

    """

    def __init__(self, textsearchterms, rangefilters=None):
        self.textsearchterms = textsearchterms
        self.rangefilters = rangefilters or {}
        self._statscache = {}
    
    def iterterms(self):
        """iterate through all text terms
        
        >>> textterms = {'f': [['first'], ['second']], 'o': [['third']]}
        >>> doc = Document(textterms)
        >>> sorted(list(doc.iterterms()))
        ['first', 'second', 'third']
        """
        return chain(*chain(*self.textsearchterms.itervalues()))

    def _stats(self, field):
        stats = self._statscache.get(field)
        if stats is not None: return stats
        tfs = defaultdict(int)
        doclen = 0
        for fieldentry in self.textsearchterms.get(field, ()):
            doclen += len(fieldentry)
            for term in fieldentry:
                tfs[term] += 1
        stats = (tfs, doclen)
        self._statscache[field] = stats
        return stats
    
    def termfreq_and_length(self, *fields):
        """Calculate term frequency and length (number of terms) for the
        fields passed.

        Note that per-field stats are cached.
        """
        tfs, doclen = self._stats(fields[0])
        tfs = dict(tfs)
        for field in fields[1:]:
            field_doclen, field_tfs = self._stats(field)
            doclen += field_doclen
            for term, freq in field_tfs:
                tfs[term] = freq + tfs.get(term, 0)
        return tfs, doclen       

    def totuple(self):
        return (self.textsearchterms, self.rangefilters)
    
    @classmethod
    def fromtuple(cls, data):
        return cls(*data)
    
    def __str__(self):
        args = ["textsearchterms=%r" % self.textsearchterms]
        if self.rangefilters:
            args.append("rangefilters=%r" % self.rangefilters)
        return "Document(%s)" % ','.join(args)
    
    __repr__ = __str__
