from collections import namedtuple

PHASE0 = namedtuple("PHASE0", ["type", "acceptable", "phase", "sender"])
PHASE1LOCK = namedtuple("PHASE1LOCK", ["type", "item", "phase", "evidence", "sender"])
PHASE2ACK = namedtuple("PHASE2ACK", ["type", "item", "phase", "sender"])
RELEASE3 = namedtuple("RELEASE3", ["type", "evidence", "sender"])


class dls_state_machine():
    """ A class representing all state of the DLS state machine for a single round. """

    PHASE0 = "PHASE0"
    PHASE1LOCK = "PHASE1LOCK"
    PHASE2ACK = "PHASE2ACK"
    RELEASE3 = "RELEASE3"

    def __init__(self, my_vi, my_id, N):
        assert 0 <= my_id < N 
        self.i = my_id 

        self.vi = my_vi
        self.N = N
        self.f = (N-1) // 3

        self.round = 0
        self.locks = {} # Contains locks associated with proof of acceptability.
        self.all_seen = set([ my_vi ])

        # In and out buffers for network IO.
        self.buf_in = set()
        self.buf_out = set()

        self.decision = None

    def get_phase_k(self, xround):
        """ Returns the ever increasing phase number. """
        return xround // 4

    def get_leader(self, xround):
        """ Returns the leader for the phase. """
        return self.get_leader_phase( self.get_phase_k(xround) )

    def get_leader_phase(self, phase):
        return phase % self.N        

    def get_round_type(self, xround):
        """ Returns the round type as where 0-2 are trying0-2 and 3 is lock-release. """
        return xround % 4

    def check_phase1msg(self, msg):

        # Check the basic format.
        if not (msg.type == self.PHASE1LOCK and len(msg) == 5):
            return False

        (_, item, k, evidence, sender) = msg

        # Only the leader can send in a specific phase.
        if not (sender == self.get_leader_phase(k)):
            return False

        # Check all votes are valid.
        votes = set()
        for e in evidence:
            if not (e[0] == self.PHASE0 and len(e) == 4):
                return False

            (_, acc, kp, sender2) = e
            if not (kp == k and item in acc):
                return False

            votes.add(sender2)

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
                    acceptable = msg.acceptable

                    for acc in acceptable:
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
        for l in self.locks:
            assert len(self.locks[l]) == 5

            msg = RELEASE3(self.RELEASE3, self.locks[l], self.i)
            self.buf_out.add( msg )
            self.buf_in.add( msg )

    # Those can be run at all phases!
    def process_release_locks(self):
        for msg in self.buf_in:
            if msg.type == self.RELEASE3 and self.check_phase1msg(msg.evidence):
                # CHECK this is a valid PHASE1LOCK
                lock = msg.evidence
                (_, w, hp, _, _) = lock
                for v, (_, _, h,_, _) in self.locks.items():
                    if v != w and hp >= h:
                        del self.locks[v]

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
                acceptable = msg.acceptable 
                self.all_seen |= set(acceptable)

    def do_background(self):
        self.find_seen()
        self.process_release_locks()
        self.process_acks()


    def process_round(self):
        process = [ self.process_trying_0, self.process_trying_1, 
                    self.process_trying_2, self.process_lockrelease_3 ] 

        self.do_background()
        rtype = self.get_round_type(self.round)
        process[rtype]()

        self.round += 1


