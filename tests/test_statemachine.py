import sys
sys.path = [".", ".."] + sys.path

from dlsconsensus import dls_state_machine, PHASE0, PHASE1LOCK, PHASE2ACK, RELEASE3

dlsc = dls_state_machine

def test_phase0_init():
    dls = dls_state_machine(my_vi="Hello", my_id=0, N=4)

    # Ensure the fist item that is acceptable is own choice.
    dls.process_trying_0()
    assert len(dls.buf_out) == 1
    item = list(dls.buf_out)[0]
    assert item[1] == ("Hello",)

def test_phase1_empty_locks():
    buf_in = set()
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 0)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello1",), 0, 1)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello2",), 0, 2)  )    

    dls = dls_state_machine(my_vi="Hello", my_id=0, N=4)
    dls.buf_in |= buf_in

    dls.process_trying_1()
    assert len(dls.buf_out) == 0

def test_phase1_seen():
    buf_in = set()
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 0)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello1",), 0, 1)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello2",), 0, 2)  )    

    dls = dls_state_machine(my_vi="hello0", my_id=0, N=4)
    dls.buf_in |= buf_in

    dls.process_round()
    assert len(dls.buf_out) == 1 # The dlsc.PHASE0 message

    # Did we increase our acceptable set?
    assert len(dls.all_seen) == 3

def test_phase1_one_locks():
    buf_in = set()
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 0)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 1)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 2)  )    

    dls = dls_state_machine(my_vi="Hello", my_id=0, N=4)
    dls.buf_in |= buf_in

    dls.process_trying_1()
    assert len(dls.buf_out) == 1

def test_phase1_one_locks_process():
    buf_in = set()
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 0)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 1)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 2)  )    

    dls = dls_state_machine(my_vi="Hello", my_id=0, N=4)
    dls.buf_in |= buf_in

    dls.process_round()
    assert len(dls.buf_out) == 1


def test_phase1_not_turn():
    buf_in = set()
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 0)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 1)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 2)  )    

    dls = dls_state_machine(my_vi="Hello", my_id=1, N=4)
    dls.buf_in |= buf_in

    dls.process_trying_1()
    assert len(dls.buf_out) == 0

def test_phase1_one_locks_phase2():
    buf_in = set()
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 0)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 1)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0",), 0, 2)  )    

    msg = PHASE1LOCK(dlsc.PHASE1LOCK, "hello0", 0, tuple(buf_in), 0)

    dls = dls_state_machine(my_vi="Hello", my_id=0, N=4)
    dls.buf_in |= set([ msg ])

    dls.round = 2
    dls.process_round()
    assert len(dls.buf_out) == 1
    assert list(dls.buf_out)[0][0] == "PHASE2ACK"

def test_phase1_one_locks_phase2_evil():
    buf_in = set()
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0","hello1"), 0, 0)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0","hello1"), 0, 1)  )    
    buf_in.add(  PHASE0(dlsc.PHASE0, ("hello0","hello1"), 0, 2)  )    

    # That is byzantine
    msg = [ PHASE1LOCK(dlsc.PHASE1LOCK, "hello0", 0, tuple(buf_in), 0) ] 
    msg += [ PHASE1LOCK(dlsc.PHASE1LOCK, "hello1", 0, tuple(buf_in), 0) ] 

    dls = dls_state_machine(my_vi="Hello", my_id=0, N=4)
    dls.buf_in |= set( msg )

    dls.round = 2
    dls.process_round()
    assert len(dls.buf_out) == 2
    for m in dls.buf_out:
        assert m[0] == dlsc.PHASE2ACK

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
                all_messages |= n.get_messages()
        for n in nodes:
            n.put_messages(all_messages)

        #print("Round: %s, Phase: %s" % (r, nodes[0].get_phase_k(r)))
        #print([n.decision for n in nodes])
        #print([len(n.buf_in) for n in nodes])

    assert set([nx.decision for nx in nodes[:3]]) == set(["Hello1"])
    assert set([nx.decision for nx in nodes]) == set(["Hello1", None])

def test_f_failures_start_later():
    # In this test we simulate a perfectly synchronous network.
    N = 4
    nodes = []
    for i in range(N):
        dls = dls_state_machine(my_vi="Hello%s" % i, my_id=i, N=4, start_r = 1000)
        nodes += [ dls ]

    # In each round simulate behavious.
    for r in range(50):
        all_messages = set()
        for n in nodes:
            n.process_round()
            if n.i < n.N - n.f:
                all_messages |= n.get_messages()
        for n in nodes:
            n.put_messages(all_messages)

        #print("Round: %s, Phase: %s" % (r, nodes[0].get_phase_k(r)))
        #print([n.decision for n in nodes])
        #print([len(n.buf_in) for n in nodes])

    assert set([nx.decision for nx in nodes[:3]]) == set(["Hello0"])
    assert set([nx.decision for nx in nodes]) == set(["Hello0", None])

