import sys
sys.path = [".", ".."] + sys.path

from dlsconsensus import dls_net_peer, BLSASK, BLSPUT, BLSDECISION, BLSACCEPTABLE, BLSLOCK, BLSACK
from dlsconsensus import PHASE0
from dlsconsensus import dls_state_machine as dlsc
from dlsconsensus import pack, unpack

def test_init():
    peer =  dls_net_peer(my_id=0, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0")

def test_decision():
    peer =  dls_net_peer(my_id=0, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                         start_r=10)

    peer.current_block_no = 2
    peer.seq.old_blocks += [(1,2,3), (4,5,6)]
    get_msg = BLSASK(channel="Shard0", 
                     type=peer.BLSASK, 
                     sender="Client1", 
                     bno=1)

    peer.put_messages([get_msg])
    assert len(peer.output) == 1
    out_msg = list(peer.output)[0]
    out_msg[0] == "Client1"
    out_msg[1].block == (4, 5, 6)


def test_put():
    peer =  dls_net_peer(my_id=0, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                         start_r=10)

    peer.current_block_no = 2
    peer.seq.old_blocks += [(1,2,3), (4,5,6)]
    put_msg = BLSPUT(channel="Shard0", 
                     type=peer.BLSASK, 
                     sender="Client1", 
                     item=7)

    peer.put_messages([put_msg])
    assert len(peer.output) == 0
    assert peer.seq.to_be_sequenced == { 7 }
    peer.put_messages([put_msg])
    assert peer.seq.to_be_sequenced == { 7 }

    put_msg = BLSPUT(channel="Shard0", 
                     type=peer.BLSASK, 
                     sender="Client1", 
                     item=8)

    peer.put_messages([put_msg])
    assert peer.seq.to_be_sequenced == { 7, 8 }

def test_decision():
    peer =  dls_net_peer(my_id=0, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                         start_r=10)
    peerB =  dls_net_peer(my_id=1, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                         start_r=10)

    peer.current_block_no = 2
    peer.seq.old_blocks += [(1,2,3), (4,5,6)]

    # BLSDECISION   = namedtuple("BLSDECISION", ["channel", "type", "sender", "bno", "block", "signature"])
    decision_msg = BLSDECISION(channel="Shard0", 
                     type=peer.BLSDECISION, 
                     sender="B", 
                     bno=2,
                     block=(7,8),
                     signature=None)

    decision_msg = peerB.pack_and_sign(decision_msg)

    peer.put_messages([decision_msg])
    buf_in = peer.sm.buf_in
    sm = peer.sm
    assert len(buf_in) == 2
    assert set(m.type for m in buf_in) == set([ sm.PHASE0, sm.PHASE2ACK ])



def test_acceptable():
    peer =  dls_net_peer(my_id=0, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                         start_r=10)
    peerB =  dls_net_peer(my_id=1, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                         start_r=10)

    peer.current_block_no = 2
    peer.seq.old_blocks += [(1,2,3), (4,5,6)]
    sm = peer.sm
    k = sm.get_phase_k(peer.round)

    # BLSACCEPTABLE = namedtuple("BLSACCEPTABLE", ["channel", "type", "sender", "bno", "phase", "blocks", "signature"])
    acceptable_msg = BLSACCEPTABLE(channel="Shard0", 
                     type=peer.BLSACCEPTABLE, 
                     sender="B", 
                     bno=2,
                     phase=k,
                     blocks=((7,8),),
                     signature=None)

    acceptable_msg = peerB.pack_and_sign(acceptable_msg)

    peer.put_messages([acceptable_msg])
    buf_in = sm.buf_in
    
    assert len(buf_in) == 1
    assert set(m.type for m in buf_in) == set([ sm.PHASE0 ])


def test_lock():
    peer =  dls_net_peer(my_id=0, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                         start_r=10)
    peerB =  dls_net_peer(my_id=1, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                         start_r=10)
    peerC =  dls_net_peer(my_id=2, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                         start_r=10)

    peer.current_block_no = 2
    peer.seq.old_blocks += [(1,2,3), (4,5,6)]
    sm = peer.sm
    k = sm.get_phase_k(peer.round)

    # prepare the evidence
    block = (7,8)
    ev = set()
    ev.add(  peer.package_raw(PHASE0(dlsc.PHASE0, ("hello0",), k, 0, None))  )    
    ev.add(  peerB.package_raw(PHASE0(dlsc.PHASE0, ("hello0",), k, 1, None))  )    
    ev.add(  peerC.package_raw(PHASE0(dlsc.PHASE0, ("hello0",), k, 2, None))  )    


    # BLSLOCK       = namedtuple("BLSLOCK", ["channel", "type", "sender", "bno", "phase", "block", "evidence", "signature"])
    lock_msg = BLSLOCK(
                channel="Shard0", 
                type=peer.BLSLOCK, 
                sender="B", 
                bno=2,
                phase=k,
                block=block,
                evidence=tuple(e.raw for e in ev),
                signature=None)

    lock_msg = peerB.pack_and_sign(lock_msg)

    peer.put_messages([lock_msg])
    buf_in = sm.buf_in
    
    assert len(buf_in) == 2
    assert set(m.type for m in buf_in) == set([ sm.PHASE1LOCK, sm.RELEASE3 ])

def test_ack():
    peer =  dls_net_peer(my_id=0, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                         start_r=10)
    peerB =  dls_net_peer(my_id=1, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                         start_r=10)


    peer.current_block_no = 2
    peer.seq.old_blocks += [(1,2,3), (4,5,6)]
    sm = peer.sm
    k = sm.get_phase_k(peer.round)

    # BLSACK        = namedtuple("BLSACK", ["channel", "type", "sender", "bno", "phase", "block", "signature"])
    ack_msg = BLSACK(channel="Shard0", 
                     type=peer.BLSACK, 
                     sender="B", 
                     bno=2,
                     phase=k,
                     block=(7,8),
                     signature=None)

    ack_msg = peerB.pack_and_sign(ack_msg)

    peer.put_messages([ack_msg])
    buf_in = sm.buf_in
    
    assert len(buf_in) == 1
    assert set(m.type for m in buf_in) == set([ sm.PHASE2ACK ])

def test_many():

    peer = {}
    addrs=["A", "B", "C", "D"]
    for i in range(4):
        peer[addrs[i]] =  dls_net_peer(my_id=i, priv="priv", addrs=addrs, 
                             pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                             start_r=10)

    for r in range(200):
        peer["C"].sm._trace = False

        for p in addrs:
            peer[p].advance_round()
            msgs = peer[p].get_messages()
            assert len(peer[p].output) == 0

            for (dest, msg) in msgs:
                peer[dest].put_messages([ msg ])

        if set([peer[p].current_block_no for p in addrs]) == set([10]):       
            break

    assert set([peer[p].current_block_no for p in addrs]) == set([10])
    print("")
    #print([(peer[p].current_block_no, peer[p].seq.old_blocks) for p in addrs])
    print("Rounds: %s" % r)


import random

def test_many_load():

    peer = {}
    addrs=["A", "B", "C", "D"]
    for i in range(4):
        peer[addrs[i]] =  dls_net_peer(my_id=i, priv="priv", addrs=addrs, 
                             pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                             start_r=10)

    for p in addrs:
        peer[p].put_sequence("M%s" % p)

    for r in range(200):
        peer["C"].sm._trace = False

        for p in addrs:
            peer[p].advance_round()
            msgs = peer[p].get_messages()
            assert len(peer[p].output) == 0

            for (dest, msg) in msgs:
                # assert msg.signature != None
                m = pack(msg)
                m2 = unpack(m)
                peer[dest].put_messages([ m2 ])

        if set([peer[p].current_block_no for p in addrs]) == set([10]):       
            break

    out = [(peer[p].current_block_no, peer[p].seq.old_blocks) for p in addrs]
    assert set([peer[p].current_block_no for p in addrs]) == set([10])
    print("\nRounds: %s" % r)

    for px in peer.values():
        assert set( px.get_sequence() ) == set(["MA", "MB", "MC", "MD"])
