from .statemachine import dls_state_machine, PHASE0, PHASE1LOCK, PHASE2ACK, RELEASE3
dlsc = dls_state_machine


""" The state machine guarantees safety and liveness, but the actual messages it 
processes do not actally do autentication, and are not compressed. This networking
library deals with the actual message formats, signatures, and efficiencies. It drives
the state machine. """

from collections import namedtuple, defaultdict
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
        self.decisions = defaultdict(set)

        # Buffers.
        self.output = set()

        # Counters
        self.sigs = 0

    def pack_and_sign(self, msg):
        assert type(msg) in [BLSACCEPTABLE, BLSLOCK, BLSACK, BLSDECISION]
        assert msg.signature == None
        xtype = type(msg)
        bdata = msgpack.packb(msg[:-1])

        return msg._make(msg[:-1] + (bdata,))


    def package_raw(self, msg):
        # If there is already a raw message, ignore.
        if msg.raw is not None:
            return msg

        self.sigs += 1


        assert msg.sender == self.i
        our_addr = self.addrs[self.i]

        if type(msg) == PHASE0:
            #BLSACCEPTABLE = namedtuple("BLSACCEPTABLE", ["channel", "type", "sender", "bno", 
            #                           "phase", "blocks", "signature"])

            data = BLSACCEPTABLE(self.channel_id, self.BLSACCEPTABLE, our_addr, 
                    self.current_block_no, msg.phase, msg.acceptable, None)

            bls_msg = self.pack_and_sign(data)
            return PHASE0._make(msg[:-1] + ( bls_msg, ) )

        elif type(msg) == PHASE1LOCK:
            #BLSLOCK       = namedtuple("BLSLOCK", ["channel", "type", "sender", "bno", 
            #                           "phase", "block", "evidence", "signature"])

            data = BLSLOCK( self.channel_id, self.BLSLOCK, our_addr, 
                     self.current_block_no, msg.phase, msg.item, msg.evidence, None)
            bls_msg = self.pack_and_sign(data)
            
            return PHASE1LOCK._make( msg[:-1] + (bls_msg, ) )

        elif type(msg) == PHASE2ACK:
            #BLSACK        = namedtuple("BLSACK", ["channel", "type", "sender", "bno", 
            #                           "phase", "block", "signature"])

            data = BLSACK( self.channel_id, self.BLSACK, our_addr, 
                     self.current_block_no, msg.phase, msg.item, None)
            bls_msg = self.pack_and_sign(data)

            return PHASE2ACK._make( msg[:-1] + (bls_msg,) )

        elif type(msg) == RELEASE3:
            #BLSLOCK       = namedtuple("BLSLOCK", ["channel", "type", "sender", "bno", 
            #                           "phase", "block", "evidence", "signature"])

            raw = msg.evidence.raw
            assert raw != None
            return RELEASE3._make(msg[:-1] + (raw,) )

        else:
            pass

        raise Exception("Wrong type: %s" % type(msg))


    def my_addr(self):
        """ Returns the address of the peer. """
        return self.addrs[self.i]

    def i_am_leader(self, r=None):
        """ Returns whether the peer is the leader for a round r."""
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

        if d not in self.decisions[bno]:
            self.decisions[bno].add(d)

        return list(self.decisions[bno])

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

            if type(msg) == BLSACCEPTABLE:
                # Schedule the message for insertion in the next block.
                for blck in msg.blocks:
                    for m in blck:
                        self.put_sequence(m)


            # Process here messages for previous blocks.
            if type(msg) in (BLSACCEPTABLE, BLSLOCK, BLSACK, BLSASK) and (msg.bno < self.current_block_no or msg.bno > self.current_block_no):
                for d in self.build_decisions(msg.bno):
                    self.decisions[msg.bno].add(d)

                    for resp in self.addrs:
                        if resp != self.my_addr():                    
                            resp = (msg.sender, d)
                            self.output.add(resp)
                continue
        
# BLSDECISION   = namedtuple("BLSDECISION", ["channel", "type", "sender", "bno", "block", "signature"])

            elif type(msg) == BLSDECISION:

                # Always save the decisions, and be ready to replay them.
                self.decisions[msg.bno].add(msg)

                if msg.bno == self.current_block_no:

                    sender_id = self.addrs.index(msg.sender)
                    phase = self.current_state_machine.get_phase_k(self.round)

                    # Simulate both a decision and an ack.
                    sm_msg = PHASE0(dlsc.PHASE0, ( msg.block, ), phase, sender_id, raw=msg)
                    msg_ack = PHASE2ACK(dlsc.PHASE2ACK, msg.block, phase, sender_id, raw=msg)

                    self.current_state_machine.put_messages([ sm_msg, msg_ack ])
                continue

# BLSACCEPTABLE = namedtuple("BLSACCEPTABLE", ["channel", "type", "sender", "bno", "phase", "blocks", "signature"])

            elif type(msg) == BLSACCEPTABLE:
                sender_id = self.addrs.index(msg.sender)
                sm_msg = PHASE0(dlsc.PHASE0, msg.blocks, msg.phase, sender_id, raw=msg)
                self.current_state_machine.put_messages([ sm_msg ])
                continue
    
# BLSLOCK       = namedtuple("BLSLOCK", ["channel", "type", "sender", "bno", "phase", "block", "evidence", "signature"])

            elif type(msg) == BLSLOCK:
                sender_id = self.addrs.index(msg.sender)
                msg_lock = PHASE1LOCK(dlsc.PHASE1LOCK, msg.block, msg.phase, msg.evidence, sender_id, raw=msg)
                msg_release = RELEASE3(dlsc.RELEASE3, msg_lock, msg.phase, sender_id, raw=msg)
                self.current_state_machine.put_messages([ msg_lock, msg_release ])


# BLSACK        = namedtuple("BLSACK", ["channel", "type", "sender", "bno", "phase", "block", "signature"])

            elif type(msg) == BLSACK:
                sender_id = self.addrs.index(msg.sender)
                msg_ack = PHASE2ACK(dlsc.PHASE2ACK, msg.block, msg.phase, sender_id, raw=msg)
                self.current_state_machine.put_messages([ msg_ack ])


            #if self.i_am_leader():
            #    self.current_state_machine.process_round(False)


    def all_others(self):
        all_receivers = self.addrs[:]
        del all_receivers[self.i]
        return all_receivers


    def get_messages(self):
        buf_out = self.current_state_machine.get_messages()

        all_receivers = self.all_others()
        if self.i_am_leader():
            receivers = all_receivers
        else:
            leader = self.current_state_machine.get_leader(self.round)
            receivers = [ self.addrs[leader] ] 

        for msg in buf_out:
            if type(msg) == PHASE0:
                new_m = BLSACCEPTABLE(self.channel_id, self.BLSACCEPTABLE, self.my_addr(), self.current_block_no,
                    msg.phase, msg.acceptable, None)
                new_m = self.pack_and_sign(new_m)

                self.output |= set( (r, new_m) for r in receivers)

            elif type(msg) == PHASE1LOCK:
                new_m = BLSLOCK(self.channel_id, self.BLSACCEPTABLE, self.my_addr(), self.current_block_no,
                    msg.phase, msg.item, msg.evidence, None)
                new_m = self.pack_and_sign(new_m)

                self.output |= set( (r, new_m) for r in receivers)

            elif type(msg) == PHASE2ACK:
                new_m = BLSACK(self.channel_id, self.BLSACCEPTABLE, self.my_addr(), self.current_block_no,
                    msg.phase, msg.item, None)
                new_m = self.pack_and_sign(new_m)

                self.output |= set( (r, new_m) for r in receivers)

            elif type(msg) == RELEASE3:
                msg = msg.evidence.raw
                #new_m = BLSLOCK(self.channel_id, self.BLSACCEPTABLE, self.addrs[msg.sender], self.current_block_no,
                #    msg.phase, msg.item, msg.evidence, None)
                #new_m = self.pack_and_sign(new_m)

                self.output |= set( (r, new_m) for r in all_receivers)
                
            else:
                raise Exception()

        out = list(self.output)
        self.output.clear()
        assert len(self.output) == 0

        return out

    def advance_round(self):
        D = self.current_state_machine.get_decision()
        if D is None:
            # No decision reached, continue the protocol.
            # But always include previous decisions in the processing.
            self.put_messages(self.decisions[self.current_block_no])

        else:
            # Decision reached, start the new block
            self.sequence += list(D)
            self.to_be_sequenced = [item for item in self.to_be_sequenced if item not in D]
            self.old_blocks += [ D ] # TODO: add evidence

            ## TODO: Possibly reconfigure the shard here.

            # Start new block
            proposal = tuple(self.to_be_sequenced)
            self.current_block_no += 1
            self.current_state_machine = dls_state_machine(proposal, self.i, self.N, self.round)
            self.current_state_machine.make_raw = self.package_raw

            # register our own decision.
            all_receivers = self.all_others()
            D = self.build_decisions(self.current_block_no - 1)

            for d in D:
                for dest in self.all_others():
                    self.output.add( (dest, d) )

        # Make a step
        self.current_state_machine.process_round()
        self.round += 1

    # External functions for sequencing.

    def put_sequence(self, item):
        """ Schedules an item to be sequenced. """
        if item not in self.sequence and item not in self.to_be_sequenced:
            self.to_be_sequenced += [ item ]

    def get_sequence(self):
        """ Get the sequence of all items that are decided. """
        return self.sequence

