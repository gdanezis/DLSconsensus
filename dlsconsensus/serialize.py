
from .types import *

import msgpack

xtypes = [tuple, set, PHASE0, PHASE1LOCK, PHASE2ACK, RELEASE3, BLSDECISION, BLSACCEPTABLE, BLSLOCK, BLSACK, BLSASK, BLSPUT]
xmap = dict((k, i) for i, k in enumerate(xtypes))

def ext_pack(x):
    if type(x) in xtypes:
        xid = xmap[type(x)]
        return msgpack.ExtType(xid, msgpack.packb(list(x), default=ext_pack, strict_types=True))

    return x

def ext_unpack(code, data):
    data = msgpack.unpackb(data, ext_hook=ext_unpack)
    xt = xtypes[code]

    if xt == tuple:
        return tuple(data)
    elif xt == set:
        return set(data)
    else:
        return xtypes[code]._make(data)

    return msgpack.ExtType(code, data)

def pack(struct):
    return msgpack.packb(struct, default=ext_pack, strict_types=True)

def unpack(data):
    return msgpack.unpackb(data, ext_hook=ext_unpack)

def test_True():
    assert True

def test_simplest():
    from .statemachine import dls_state_machine
    data0 = PHASE0(dls_state_machine.PHASE0, ("hello0",), 0, 0, None)
    bin0  = pack(data0)
    data1 = unpack(bin0)
    assert data0 == data1

def test_list():
    from .statemachine import dls_state_machine
    p0 = PHASE0(dls_state_machine.PHASE0, ("hello0",), 0, 0, None)
    data0 = [ p0, (), [] ]
    bin0  = pack(data0)
    data1 = unpack(bin0)
    assert data0 == data1

def test_set():
    data0 = [1, set([1, 2, 3 ]), {}]
    bin0  = pack(data0)
    data1 = unpack(bin0)
    assert data0 == data1

