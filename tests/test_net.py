import sys
sys.path = [".", ".."] + sys.path

from dlsconsensus import dls_net_peer

def test_init():
	peer =  dls_net_peer(my_id=0, priv="priv", addrs=["A", "B", "C", "D"], 
						 pubs=["pubA","pubB","pubC","pubD"], channel_id="Shard0")