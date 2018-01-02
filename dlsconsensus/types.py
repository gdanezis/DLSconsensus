from collections import namedtuple

PHASE0 = namedtuple("PHASE0", ["type", "acceptable", "phase", "sender", "raw"])
PHASE1LOCK = namedtuple("PHASE1LOCK", ["type", "item", "phase", "evidence", "sender", "raw"])
PHASE2ACK = namedtuple("PHASE2ACK", ["type", "item", "phase", "sender", "raw"])
RELEASE3 = namedtuple("RELEASE3", ["type", "evidence", "phase", "sender", "raw"])

# Define here the messages

# A decision is timeless, no need to specify a round number. It is also addressed to all.
BLSDECISION   = namedtuple("BLSDECISION", ["channel", "type", "sender", "bno", "block", "signature"])
BLSACCEPTABLE = namedtuple("BLSACCEPTABLE", ["channel", "type", "sender", "bno", "phase", "blocks", "signature"])
BLSLOCK       = namedtuple("BLSLOCK", ["channel", "type", "sender", "bno", "phase", "block", "evidence", "signature"])
BLSACK        = namedtuple("BLSACK", ["channel", "type", "sender", "bno", "phase", "block", "signature"])

# User facing actions. No authentication needed.
BLSASK        = namedtuple("BLSASK", ["channel", "type", "sender", "bno"])
BLSPUT        = namedtuple("BLSPUT", ["channel", "type", "sender", "item"])
