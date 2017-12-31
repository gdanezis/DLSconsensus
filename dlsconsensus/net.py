from .statemachine import dls_state_machine, PHASE0, PHASE1LOCK, PHASE2ACK, RELEASE3

""" The state machine guarantees safety and liveness, but the actual messages it 
processes do not actally do autentication, and are not compressed. This networking
library deals with the actual message formats, signatures, and efficiencies. It drives
the state machine. """

import msgpack



class dls_net_peer():

    def __init__(self, my_id, priv, addrs, pubs, channel_id):
        assert len(addrs) == len(pubs)
        self.N = len(addrs)

        assert 0 <= my_id < self.N
        self.i = my_id
        self.priv = priv

        self.addrs = addrs
        self.pubs = pubs

        self.channel_id = channel_id

        # Messages to be sequenced.
        self.to_be_sequenced = []
        self.sequence = []

        # Blocks
        self.current_block_no = 0
        self.current_state_machine = None
        self.old_blocks = []

    # Internal functions for IO.
    def put_messages(self, msgs):
        pass

    def get_messages(self):
        pass

    def advance_round(self):
        pass

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

