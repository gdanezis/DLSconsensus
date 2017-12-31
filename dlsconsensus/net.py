from .statemachine import dls_state_machine, PHASE0, PHASE1LOCK, PHASE2ACK, RELEASE3
dlsc = dls_state_machine


""" The state machine guarantees safety and liveness, but the actual messages it 
processes do not actally do autentication, and are not compressed. This networking
library deals with the actual message formats, signatures, and efficiencies. It drives
the state machine. """

from collections import namedtuple
import msgpack

# Define here the messages

# A decision is timeless, no need to specify a round number. It is also addressed to all.
BLSDECISION   = namedtuple("BLSDECISION", ["channel", "type", "sender", "bno", "block", "signature"])
BLSACCEPTABLE = namedtuple("BLSACCEPTABLE", ["channel", "type", "sender", "bno", "phase", "blocks", "signature"])
BLSLOCK       = namedtuple("BLSLOCK", ["channel", "type", "sender", "bno", "phase", "block", "evidence", "signature"])
BLSACK        = namedtuple("BLSACK", ["channel", "type", "sender", "bno", "phase", "block", "signature"])

# User facing actions. No authentication needed.
BLSASK        = namedtuple("BLSASK", ["channel", "type", "sender", "bno"])
BLSPUT        = namedtuple("BLSPUT", ["channel", "type", "sender", "item"])


class dls_net_peer():

    BLSDECISION = "BLSDECISION"
    BLSACCEPTABLE = "BLSACCEPTABLE"
    BLSLOCK = "BLSLOCK"
    BLSACK = "BLSACK"
    BLSASK = "BLSASK"
    BLSPUT = "BLSPUT"

    def __init__(self, my_id, priv, addrs, pubs, channel_id, start_r=0):
        assert len(addrs) == len(pubs)
        self.N = len(addrs)

        assert 0 <= my_id < self.N
        self.i = my_id
        self.priv = priv

        self.addrs = addrs
        self.pubs = pubs

        self.channel_id = channel_id
        self.round =  start_r

        # Messages to be sequenced.
        self.to_be_sequenced = []
        self.sequence = []

        # Blocks
        self.current_block_no = 0
        self.current_state_machine = dls_state_machine((), self.i, self.N, self.round)
        self.old_blocks = []

        # Buffers.
        self.output = set()

    def my_addr(self):
        return self.addrs[self.i]

    def i_am_leader(self, r=None):
        if r is None:
            r = self.round
        return self.current_state_machine.get_leader(r) == self.i


    def build_decisions(self, bno):
        # TODO: eventually store all decisions and memoize.
        if not (bno < self.current_block_no):
            return []

        d = BLSDECISION(channel = self.channel_id, 
                        type  = self.BLSDECISION,
                        sender  = self.my_addr(),
                        bno     =  bno,
                        block   = self.old_blocks[bno],
                        signature = None)
        return [ d ]

    def insert_item(self, put_msg):
        if put_msg.item not in self.sequence and put_msg.item not in self.to_be_sequenced:
            self.to_be_sequenced += [ put_msg.item ]

    # Internal functions for IO.
    def put_messages(self, msgs):
        for msg in msgs:
            if msg.channel != self.channel_id:
                continue

            if type(msg) == BLSPUT:
                # Schedule the message for insertion in the next block.
                self.insert_item(msg)
                continue

            # Process here messages for previous blocks.
            elif msg.bno < self.current_block_no:
                for d in self.build_decisions(msg.bno):
                    resp = (msg.sender, d)
                    self.output.add(resp)
                continue
        
# BLSDECISION   = namedtuple("BLSDECISION", ["channel", "type", "sender", "bno", "block", "signature"])

            elif type(msg) == BLSDECISION:
                sender_id = self.addrs.index(msg.sender)
                phase = self.current_state_machine.get_leader(self.round)
                sm_msg = PHASE0(dlsc.PHASE0, [ self.block ], phase, sender_id)
                self.current_state_machine.put_messages([ sm_msg ])
                continue

# BLSACCEPTABLE = namedtuple("BLSACCEPTABLE", ["channel", "type", "sender", "bno", "phase", "blocks", "signature"])

            elif type(msg) == BLSACCEPTABLE:
                sender_id = self.addrs.index(msg.sender)
                sm_msg = PHASE0(dlsc.PHASE0, self.blocks, self.phase, sender_id)
                self.current_state_machine.put_messages([ sm_msg ])
                continue
    
# BLSLOCK       = namedtuple("BLSLOCK", ["channel", "type", "sender", "bno", "phase", "block", "evidence", "signature"])

            elif type(msg) == BLSLOCK:
                sender_id = self.addrs.index(msg.sender)
                msg_lock = PHASE1LOCK(dlsc.PHASE1LOCK, msg.block, msg.evidence, sender_id)
                msg_release = RELEASE3(dlsc.RELEASE3, msg_lock)
                self.current_state_machine.put_messages([ msg_lock, msg_release ])


# BLSACK        = namedtuple("BLSACK", ["channel", "type", "sender", "bno", "phase", "block", "signature"])

            elif type(msg) == BLSACK:
                sender_id = self.addrs.index(msg.sender)
                msg_ack = PHASE2ACK(dlsc.PHASE2ACK, msg.block, msg.phase, sender_id)


            if self.i_am_leader():
                self.current_state_machine.process_round(False)

# PHASE0 = namedtuple("PHASE0", ["type", "acceptable", "phase", "sender"])
# PHASE1LOCK = namedtuple("PHASE1LOCK", ["type", "item", "phase", "evidence", "sender"])
# PHASE2ACK = namedtuple("PHASE2ACK", ["type", "item", "phase", "sender"])
# RELEASE3 = namedtuple("RELEASE3", ["type", "evidence", "phase", "sender"])


    def get_messages(self):
        pass

    def advance_round(self):
        D = self.current_state_machine.get_decision()
        if D is None:
            # No decision reached, continue the protocol.
            pass
        else:
            # Decision reached, start the new block
            self.sequence += list(D)
            self.to_be_sequenced = [item for item in self.to_be_sequenced if item not in D]
            self.old_blocks += [ D ] # TODO: add evidence

            ## Possibly reconfigure the shard here.

            # Start new block
            proposal = tuple(self.to_be_sequenced)
            self.current_block_no += 1
            self.current_state_machine = dls_state_machine(proposal, self.i, self.N, self.round)

        # Make a step
        self.current_state_machine.process_round()
        self.round += 1

    # External functions for sequencing.

    def sequence(self, item):
        """ Schedules an item to be sequenced. """
        if item not in self.sequence:
            self.to_be_sequenced += [ item ]

    def get_sequence(self):
        """ Get the sequence of all items that are decided. """
        return self.sequence

    def initiate_new_dls_session(self):
        pass

