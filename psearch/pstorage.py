"""
Storage

Implementations of storage engines for prospective search
"""
import gdbm, cPickle, sys
from itertools import chain, izip
import numpy as np

# numeric python datatype for stored query and mask
_pdtype = np.int32

class MemoryStore(object):
    """Memory storage
    
    An entire copy of all data is held in memory.

    If a file name is provided, then data is written to a file when the store
    is closed and readmode=False and data is read from fname if opened with
    readmode=True. 

    If fname is '-', data is written to standard output.
    """
    def __init__(self, fname=None, readmode=False):
        self.fname = fname
        self.readmode = readmode
        if readmode and fname is not None:
            pfile = open(fname, 'rb')
            self.postmap = cPickle.load(pfile)
            self.data = cPickle.load(pfile)
        else:
            self.postmap = {}
            self.data = {}
    
    def close(self):
        if self.fname is not None and not self.readmode:
            pfile = open(self.fname, 'wb') if self.fname != '-' else sys.stdout
            cPickle.dump(self.postmap, pfile, 2)
            cPickle.dump(self.data, pfile, 2)

    def write_posts(self, prefix, term, values):
        self.postmap[(prefix, term)] = list(values)

    def read_posts(self, prefix, term):
        return self.postmap.get((prefix, term), ())

    def set_data(self, qid, data):
        self.data[qid] = data

    def get_data(self, qid, default=None):
        return self.data.get(qid, default)

    def iteritems(self):
        return ((prefix, term, value) for ((prefix, term), value) in self.postmap.iteritems())

class TCHStore(object):
    """Storage based on Tokyo Cabinet hash storage"""
    def __init__(self, fname, readmode=False):
        import pytc
        self.fname = fname
        self.db = pytc.HDB()
        flags = pytc.HDBOREADER if readmode else pytc.HDBOWRITER | pytc.HDBOCREAT
        self.db.open(fname, flags)

    def write_posts(self, prefix, term, values):
        termstr = "%s%s" % (prefix, term)
        posting = np.fromiter(chain(*values), np.int32).tostring()
        self.db.putasync(termstr, posting)
    
    def read_posts(self, prefix, term):
        try:
            data = self.db["%s%s" % (prefix, term)]
        except KeyError:
            return ()
        else:
            abuf = np.frombuffer(data, _pdtype)
            return izip(abuf[::2], abuf[1::2])
    
    def close(self):
        self.db.close()

    def set_data(self, qid, data):
        self.db.putasync("_%s" % qid, cPickle.dumps(data, 2))

    def get_data(self, qid, default=None):
        try:
            data = self.db["_%s" % qid]
            return cPickle.loads(data)
        except KeyError:
            return default
    
    def iteritems(self):
        for k in self.db:
            part_type = k[0]
            if part_type != '_':
                key = k[1:]
                value = self.read_posts(part_type, key)
                yield part_type, key, value 

class GDBMStore(object):
    """Storage engine based on GDBM"""
    def __init__(self, fname, readmode=False):
        self.fname = fname
        self.readmode = readmode
        openmode = 'r' if readmode else 'n'
        self.idxdb = gdbm.open(fname, openmode)
    
    def write_posts(self, prefix, term, values):
        termstr = "%s%s" % (prefix, term)
        posting = np.fromiter(chain(*values), _pdtype).tostring()
        self.idxdb[termstr] = posting
    
    def read_posts(self, prefix, term):
        try:
            data = self.idxdb["%s%s" % (prefix, term)]
        except KeyError:
            return ()
        else:
            abuf = np.frombuffer(data, _pdtype)
            return izip(abuf[::2], abuf[1::2])

    def close(self):
        self.idxdb.close()

    def set_data(self, qid, data):
        self.idxdb["_%s" % qid] = cPickle.dumps(data, 2)

    def get_data(self, qid, default=None):
        try:
            data = self.idxdb["_%s" % qid]
            return cPickle.loads(data)
        except KeyError:
            return default
    
    def iteritems(self):
        k = self.idxdb.firstkey()
        while k != None:
            part_type = k[0]
            if part_type != '_':
                key = k[1:]
                value = self.read_posts(part_type, key)
                yield part_type, key, value 
            k = self.idxdb.nextkey(k)
