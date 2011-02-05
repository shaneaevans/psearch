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
        termfilters: all fields that may be used as filters and their terms
            {'material': ['leather', 'faux leather']}
        rangefilters: a dict of fields and values that may be used as range filters.
            {'price', [100.0, 200.0]}
    each is a dictionary mapping from field name to a list of values.

    """

    def __init__(self, textsearchterms, termfilters=None, rangefilters=None):
        self.textsearchterms = textsearchterms
        self.termfilters = termfilters or {}
        self.rangefilters = rangefilters or {}
        self._statscache = {}
    
    def itertextterms(self):
        """iterate through all text terms

        This ignores terms that participate in field queries
        """
        return chain(*chain(*self.textsearchterms.itervalues()))

    def iterprefixedterms(self):
        """iterate through all terms

        >>> textterms = {'f': [['first'], ['second']], 'o': [['third']]}
        >>> doc = Document(textterms)
        >>> sorted(list(doc.iterprefixedterms()))
        ['first', 'second', 'third']

        Terms that are filters are prefixed with the field they are 
        filtering, for example "category:chairs". Terms that are 
        not prefixed are included unmodified.
        >>> textterms = {'field': [['first', 'value']]}
        >>> filterterms = {'category': ['chairs']}
        >>> doc = Document(textterms, filterterms)
        >>> list(doc.iterprefixedterms())
        ['first', 'value', 'category:chairs']

        """
        pterms = ["%s:%s" % (field, term) for (field, terms) \
                in self.termfilters.iteritems() for term in terms]
        return chain(self.itertextterms(), pterms)
    
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
        return (self.textsearchterms, self.termfilters, self.rangefilters)
    
    @classmethod
    def fromtuple(cls, data):
        return cls(*data)
    
    def __str__(self):
        args = ["textsearchterms=%r" % self.textsearchterms]
        if self.termfilters:
            args.append("termfilters=%r" % self.termfilters)
        if self.rangefilters:
            args.append("rangefilters=%r" % self.rangefilters)
        return "Document(%s)" % ','.join(args)
    
    __repr__ = __str__
