import sys
sys.path = [".", ".."] + sys.path

from dlsconsensus import dls_net_peer, BLSASK, BLSPUT

def test_init():
    peer =  dls_net_peer(my_id=0, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0")

def test_decision():
    peer =  dls_net_peer(my_id=0, priv="priv", addrs=["A", "B", "C", "D"], 
                         pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0", 
                         start_r=10)

    peer.current_block_no = 2
    peer.old_blocks += [(1,2,3), (4,5,6)]
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
    peer.old_blocks += [(1,2,3), (4,5,6)]
    put_msg = BLSPUT(channel="Shard0", 
                     type=peer.BLSASK, 
                     sender="Client1", 
                     item=7)

    peer.put_messages([put_msg])
    assert len(peer.output) == 0
    assert peer.to_be_sequenced == [ 7 ]
    peer.put_messages([put_msg])
    assert peer.to_be_sequenced == [ 7 ]

    put_msg = BLSPUT(channel="Shard0", 
                     type=peer.BLSASK, 
                     sender="Client1", 
                     item=8)

    peer.put_messages([put_msg])
    assert peer.to_be_sequenced == [ 7, 8 ]
