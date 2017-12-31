from collections import namedtuple

PHASE0 = namedtuple("PHASE0", ["type", "acceptable", "phase", "sender"])
PHASE1LOCK = namedtuple("PHASE1LOCK", ["type", "item", "phase", "evidence", "sender"])
PHASE2ACK = namedtuple("PHASE2ACK", ["type", "item", "phase", "sender"])
RELEASE3 = namedtuple("RELEASE3", ["type", "evidence", "phase", "sender"])

valid_messages = set([ PHASE0, PHASE1LOCK, PHASE2ACK, RELEASE3 ])

class dls_state_machine():
    """ A class representing all state of the DLS state machine for a single round. """

    PHASE0 = "PHASE0"
    PHASE1LOCK = "PHASE1LOCK"
    PHASE2ACK = "PHASE2ACK"
    RELEASE3 = "RELEASE3"

    def __init__(self, my_vi, my_id, N, start_r = 0):
        """ Initialize with an own value, own id and the number of peers. """
        assert 0 <= my_id < N 
        self.i = my_id 

        self.vi = my_vi
        self.N = N
        self.f = (N-1) // 3

        self.round = start_r
        self.locks = {} # Contains locks associated with proof of acceptability.
        self.all_seen = set([ my_vi ])

        # In and out buffers for network IO.
        self.buf_in = set()
        self.buf_out = set()

        self.decision = None

    def get_decision(self):
        return self.decision

    def get_phase_k(self, xround):
        """ Returns the ever increasing phase number. """
        return xround // 4

    def get_leader(self, xround):
        """ Returns the leader for the phase. """
        return self.get_leader_phase( self.get_phase_k(xround) )

    def get_leader_phase(self, phase):
        """ Get the leader for a particular phase. """
        return phase % self.N        

    def get_round_type(self, xround):
        """ Returns the round type as where 0-2 are trying0-2 and 3 is lock-release. """
        return xround % 4

    def check_phase1msg(self, msg):

        # Check the basic format.
        if not (msg.type == self.PHASE1LOCK and len(msg) == 5):
            return False

        # Only the leader can send in a specific phase.
        if not (msg.sender == self.get_leader_phase(msg.phase)):
            return False

        # Check all votes are valid.
        votes = set()
        for e in msg.evidence:
            if not (e.type == self.PHASE0 and len(e) == 4):
                return False

            if not (e.phase == msg.phase and msg.item in e.acceptable):
                return False

            votes.add(e.sender)

        # Check quorum
        if not (len(votes) >= (self.N - self.f)):
            return False

        return True

    def get_acceptable(self):
        if self.decision != None:
            return ( self.decision, )

        if len(self.locks) == 0:
            return tuple(self.all_seen)
        elif len(self.locks) == 1:
            return ( self.locks.keys()[0], )
        else:
            print (len(self.locks), self.locks)
            assert False


    def process_trying_0(self):
        acceptable = self.get_acceptable()
        k = self.get_phase_k(self.round)
        msg = PHASE0(self.PHASE0, acceptable, k, self.i)
        self.buf_out |= set(( msg, ))

        if self.i == self.get_leader(self.round):
            self.buf_in.add(msg)


    def process_trying_1(self):
        # Get all PHASE 1 messages for current phase.
        if self.i == self.get_leader(self.round):

            k = self.get_phase_k(self.round)
            evidence = {}

            for msg in self.buf_in:
                if msg.type == self.PHASE0 and msg.phase == k:
                    for acc in msg.acceptable:
                        if acc not in evidence:
                            evidence[acc] = (set(), set())
                        
                        votes, evid_set = evidence[acc]
                        votes |= set([ msg.sender ])
                        evid_set |= set([ msg ])

            # Prune those with enough evidence:
            for acc in evidence.keys():
                votes, _ = evidence[acc]
                if len(votes) < self.N - self.f:
                    del evidence[acc]

            if len(evidence) > 0:
                if self.vi in evidence:
                    # prefer our own.
                    item = self.vi
                else:
                    # Chose arbitrarily.
                    item = max(evidence)

                evidence = tuple(evidence[item][1])
                msg = PHASE1LOCK(self.PHASE1LOCK, item, k, evidence, self.i)
                self.buf_in.add(msg)
                self.buf_out.add(msg)



    def process_trying_2(self):
        k = self.get_phase_k(self.round)
        for msg in list(self.buf_in):
            if msg.type == self.PHASE1LOCK and msg.phase == k and self.check_phase1msg(msg):
                item = msg.item

                self.locks[item] = msg

                ack = PHASE2ACK(self.PHASE2ACK, item, k, self.i)
                self.buf_out.add(ack)

                if self.i == self.get_leader(self.round):
                    self.buf_in.add(ack)


    def process_lockrelease_3(self):
        k = self.get_phase_k(self.round)
        for l in self.locks:
            assert len(self.locks[l]) == 5

            msg = RELEASE3(self.RELEASE3, self.locks[l], k, self.i)
            self.buf_out.add( msg )
            self.buf_in.add( msg )

    # Those can be run at all phases!
    def process_release_locks(self):
        for msg in self.buf_in:
            if msg.type == self.RELEASE3 and self.check_phase1msg(msg.evidence):
                new_lock = msg.evidence
                for old_lock in self.locks.values():
                    if old_lock.item != new_lock.item and new_lock.phase >= old_lock.phase:
                        del self.locks[old_lock.item]

    def process_acks(self):
        all_acks = {}
        for msg in self.buf_in:
            # Only process acks for own phases
            if msg.type == self.PHASE2ACK and self.get_leader_phase(msg.phase) == self.i:
                if msg.item not in all_acks:
                    all_acks[msg.item] = set()
                all_acks[msg.item].add( msg.sender )

                if len(all_acks[msg.item]) >= self.N - self.f:
                    self.decision = msg.item

    def find_seen(self):
        for msg in self.buf_in:
            if msg.type == self.PHASE0:
                self.all_seen |= set(msg.acceptable)

    def clear_old_messages(self):
        k = self.get_phase_k(self.round)
        for msg in list(self.buf_in):
            if msg.phase < k:
                self.buf_in.remove(msg)


    def do_background(self):
        self.find_seen()
        self.process_release_locks()
        self.clear_old_messages()
        self.process_acks()

    def process_round(self, advance=True):
        """ Run one round of the state machine. """
        process = [ self.process_trying_0, self.process_trying_1, 
                    self.process_trying_2, self.process_lockrelease_3 ] 

        self.do_background()
        rtype = self.get_round_type(self.round)
        process[rtype]()

        if advance:
            self.round += 1
        return self.round

    def put_messages(self, msgs):
        """ Insert a set of messages into the state machine. """
        if not all([ type(m) in valid_messages for m in msgs]):
            raise Exception("Wrong input type.")

        self.buf_in |= set(msgs)


    def get_messages(self):
        """ Get all the messages emited by the state machine. """
        msgs = self.buf_out.copy()
        self.buf_out.clear()
        return msgs

