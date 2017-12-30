
class dls_state_machine():
    """ A class representing all state of the DLS state machine for a single round. """

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
        return True # TODO: check this!!!

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
        msg = ("PHASE0", acceptable, k, self.i)
        self.buf_out |= set(( msg, ))

        if self.i == self.get_leader(self.round):
            self.buf_in.add(msg)


    def process_trying_1(self):
        # Get all PHASE 1 messages for current phase.
        if self.i == self.get_leader(self.round):

            k = self.get_phase_k(self.round)
            evidence = {}

            for msg in self.buf_in:
                if msg[0] == "PHASE0" and msg[2] == k:
                    acceptable = msg[1]

                    for acc in acceptable:
                        if acc not in evidence:
                            evidence[acc] = (set(), set())
                        
                        votes, evid_set = evidence[acc]
                        votes |= set([ msg[3] ])
                        evid_set |= set([ msg ])

            # Prune those with enough evidence:
            for acc in evidence.keys():
                if len(evidence[acc][0]) < self.N - self.f:
                    del evidence[acc]

            if len(evidence) > 0:
                # Chose arbitrarily.
                item = list(sorted(evidence))[0]
                msg = ("PHASE1LOCK", item, k, tuple(evidence[item][1]), self.i)
                self.buf_in.add(msg)
                self.buf_out.add(msg)



    def process_trying_2(self):
        k = self.get_phase_k(self.round)
        for msg in list(self.buf_in):
            if msg[0] == "PHASE1LOCK" and msg[2] == k and self.check_phase1msg(msg):
                item = msg[1]

                self.locks[item] = msg

                ack = ("PHASE2ACK", item, k, self.i)
                self.buf_out.add(ack)

                if self.i == self.get_leader(self.round):
                    self.buf_in.add(ack)


    def process_lockrelease_3(self):
        for l in self.locks:
            assert len(self.locks[l]) == 5

            msg = ("RELEASE3", self.locks[l], self.i)
            self.buf_out.add( msg )
            self.buf_in.add( msg )

    # Those can be run at all phases!
    def process_release_locks(self):
        for msg in self.buf_in:
            if msg[0] == "RELEASE3" and self.check_phase1msg(msg[1]):
                # CHECK this is a valid PHASE1LOCK
                lock = msg[1]
                (_, w, hp, _, _) = lock
                for v, (_, _, h,_, _) in self.locks.items():
                    if v != w and hp >= h:
                        del self.locks[v]

    def process_acks(self):
        all_acks = {}
        for msg in self.buf_in:
            # Only process acks for own phases
            if msg[0] == "PHASE2ACK" and self.get_leader_phase(msg[2]) == self.i:
                (_, item, k, i) = msg
                if item not in all_acks:
                    all_acks[item] = set()
                all_acks[item].add( i )

                if len(all_acks[item]) >= self.N - self.f:
                    if self.decision != None:
                        assert item == self.decision

                    self.decision = item

    def find_seen(self):
        for msg in self.buf_in:
            if msg[0] == "PHASE0":
                acceptable = msg[1] 
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




def test_phase0_init():
    dls = dls_state_machine(my_vi="Hello", my_id=0, N=4)

    # Ensure the fist item that is acceptable is own choice.
    dls.process_trying_0()
    assert len(dls.buf_out) == 1
    item = list(dls.buf_out)[0]
    assert item[1] == ("Hello",)

def test_phase1_empty_locks():
    buf_in = set()
    buf_in.add(  ("PHASE0", ("hello0",), 0, 0)  )    
    buf_in.add(  ("PHASE0", ("hello1",), 0, 1)  )    
    buf_in.add(  ("PHASE0", ("hello2",), 0, 2)  )    

    dls = dls_state_machine(my_vi="Hello", my_id=0, N=4)
    dls.buf_in |= buf_in

    dls.process_trying_1()
    assert len(dls.buf_out) == 0

def test_phase1_seen():
    buf_in = set()
    buf_in.add(  ("PHASE0", ("hello0",), 0, 0)  )    
    buf_in.add(  ("PHASE0", ("hello1",), 0, 1)  )    
    buf_in.add(  ("PHASE0", ("hello2",), 0, 2)  )    

    dls = dls_state_machine(my_vi="hello0", my_id=0, N=4)
    dls.buf_in |= buf_in

    dls.process_round()
    assert len(dls.buf_out) == 1 # The "PHASE0" message

    # Did we increase our acceptable set?
    assert len(dls.all_seen) == 3

def test_phase1_one_locks():
    buf_in = set()
    buf_in.add(  ("PHASE0", ("hello0",), 0, 0)  )    
    buf_in.add(  ("PHASE0", ("hello0",), 0, 1)  )    
    buf_in.add(  ("PHASE0", ("hello0",), 0, 2)  )    

    dls = dls_state_machine(my_vi="Hello", my_id=0, N=4)
    dls.buf_in |= buf_in

    dls.process_trying_1()
    assert len(dls.buf_out) == 1

def test_phase1_one_locks_process():
    buf_in = set()
    buf_in.add(  ("PHASE0", ("hello0",), 0, 0)  )    
    buf_in.add(  ("PHASE0", ("hello0",), 0, 1)  )    
    buf_in.add(  ("PHASE0", ("hello0",), 0, 2)  )    

    dls = dls_state_machine(my_vi="Hello", my_id=0, N=4)
    dls.buf_in |= buf_in

    dls.process_round()
    assert len(dls.buf_out) == 1


def test_phase1_not_turn():
    buf_in = set()
    buf_in.add(  ("PHASE0", ("hello0",), 0, 0)  )    
    buf_in.add(  ("PHASE0", ("hello0",), 0, 1)  )    
    buf_in.add(  ("PHASE0", ("hello0",), 0, 2)  )    

    dls = dls_state_machine(my_vi="Hello", my_id=1, N=4)
    dls.buf_in |= buf_in

    dls.process_trying_1()
    assert len(dls.buf_out) == 0

def test_phase1_one_locks_phase2():
    buf_in = set()
    buf_in.add(  ("PHASE0", ("hello0",), 0, 0)  )    
    buf_in.add(  ("PHASE0", ("hello0",), 0, 1)  )    
    buf_in.add(  ("PHASE0", ("hello0",), 0, 2)  )    

    msg = ("PHASE1LOCK", "hello0", 0, tuple(buf_in), 0)

    dls = dls_state_machine(my_vi="Hello", my_id=0, N=4)
    dls.buf_in |= set([ msg ])

    dls.round = 2
    dls.process_round()
    assert len(dls.buf_out) == 1
    assert list(dls.buf_out)[0][0] == "PHASE2ACK"

def test_phase1_one_locks_phase2_evil():
    buf_in = set()
    buf_in.add(  ("PHASE0", ("hello0","hello1"), 0, 0)  )    
    buf_in.add(  ("PHASE0", ("hello0","hello1"), 0, 1)  )    
    buf_in.add(  ("PHASE0", ("hello0","hello1"), 0, 2)  )    

    # That is byzantine
    msg = [ ("PHASE1LOCK", "hello0", 0, tuple(buf_in), 0) ] 
    msg += [ ("PHASE1LOCK", "hello1", 0, tuple(buf_in), 0) ] 

    dls = dls_state_machine(my_vi="Hello", my_id=0, N=4)
    dls.buf_in |= set( msg )

    dls.round = 2
    dls.process_round()
    assert len(dls.buf_out) == 2
    for m in dls.buf_out:
        assert m[0] == "PHASE2ACK"

    # Does it have 2 locks?
    assert len(dls.locks) == 2

    dls.process_round()
    dls.process_round()
    
    assert len(dls.locks) == 0

def test_perfect_net_conditions():
    # In this test we simulate a perfectly synchronous network.
    N = 4
    nodes = []
    for i in range(N):
        dls = dls_state_machine(my_vi="Hello%s" % i, my_id=i, N=4)
        nodes += [ dls ]

    # In each round simulate behavious.
    for r in range(50):
        all_messages = set()
        for n in nodes:
            n.process_round()
            all_messages |= n.buf_out
        for n in nodes:
            n.buf_in |= all_messages

    assert len(set([nx.decision for nx in nodes])) == 1


def test_f_failures():
    # In this test we simulate a perfectly synchronous network.
    N = 4
    nodes = []
    for i in range(N):
        dls = dls_state_machine(my_vi="Hello%s" % i, my_id=i, N=4)
        nodes += [ dls ]

    # In each round simulate behavious.
    for r in range(50):
        all_messages = set()
        for n in nodes:
            n.process_round()
            if n.i < n.N - n.f:
                all_messages |= n.buf_out
        for n in nodes:
            n.buf_in |= all_messages

    assert set([nx.decision for nx in nodes[:3]]) == set(["Hello0"])
    assert set([nx.decision for nx in nodes]) == set(["Hello0", None])