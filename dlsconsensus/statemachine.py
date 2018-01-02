from hashlib import sha256

from .types import *
from .serialize import pack, unpack

valid_messages = set([ PHASE0, PHASE1LOCK, PHASE2ACK, RELEASE3 ])


class dls_state_machine():
    """ A class representing all state of the DLS state machine for a single round. """

    PHASE0 = "PHASE0"
    PHASE1LOCK = "PHASE1LOCK"
    PHASE2ACK = "PHASE2ACK"
    RELEASE3 = "RELEASE3"

    def __init__(self, my_vi, my_id, N, start_r = 0, make_raw = None, backup_f = None):
        """ Initialize with an own value, own id and the number of peers. """
        assert 0 <= my_id < N 

        # This is the key state that needs to be saved to persit the instance
        self.i = my_id 
        self.vi = my_vi
        self.N = N
        self.all_seen = set([ my_vi ])
        self.round = start_r
        self.locks = {} # Contains locks associated with proof of acceptability.
        self.decision = None

        self.f = (N-1) // 3

        # In and out buffers for network IO.
        self.buf_in = set()
        self.buf_out = set()

        self._trace = False

        self.make_raw = make_raw
        if self.make_raw == None:
            self.make_raw = lambda x: x

        self.backup_f = backup_f
        
    def persist(self):
        data = (self.i, self.vi, self.N, self.all_seen, self.round, self.locks, self.decision)
        
        bindata = pack(data)
        binhash = sha256(bindata).digest()[:16]

        if self._trace:
            print("Persist %s bytes" % len(bindata + binhash))

        if self.backup_f is not None:
            for f1 in self.backup_f:
                f1.seek(0)
                f1.write(bindata + binhash)
                f1.truncate()
                f1.flush()

            if __debug__:
                data2 = self.recover_from_f(f1)
    
    def recover_from_f(self, f1):
        f1.seek(0)
        raw = f1.read()
        bindata = raw[:-16]
        binhash = raw[-16:]
        if sha256(bindata).digest()[:16] != binhash:
            raise Excpetion("Digest mismatch")

        data = unpack(bindata)
        return data

    def recover(self, just_check = False):
        if self.backup_f is None:
            raise Exception("No backup files available.")

        errors = False

        recovered = []
        for f1 in self.backup_f:
            try:
                data = self.recover_from_f(f1)
                recovered += [(data[4], data)]
            except:
                errors = True            
             
        if len(recovered) == 0:
            raise Exception("All backups failed.")

        # Take the higher round backup
        data = max(recovered)[1]

        if not just_check:
            # Assign it
            self.i, self.vi, self.N, self.all_seen, \
                self.round, self.locks, self.decision = data
        else:
            # Check it is equal
            all_ok = (self.i, self.vi, self.N, self.all_seen, \
                self.round, self.locks, self.decision) == data

            if not all_ok:
                print("Mismatch:")
                print((self.i, self.vi, self.N, self.all_seen, self.round, self.locks, self.decision))
                print
                print(data)
                raise Exception("Backups are failing")

        if errors:
            # Since one or more of the backups were broken, we are now re-writing them.
            pass # TODO

    def set_make_raw(self, maker):
        """ Set a function that packages the messages, with signatures, etc. """
        self.make_raw =  maker

    def get_decision(self):
        """ Get the decision for this state machine or None """
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
        if not (msg.type == self.PHASE1LOCK):
            return False

        # Only the leader can send in a specific phase.
        if not (msg.sender == self.get_leader_phase(msg.phase)):
            return False

        # Check all votes are valid.
        votes = set()
        for e in msg.evidence:
            if not (e.type == self.PHASE0):
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
        msg = PHASE0(self.PHASE0, acceptable, k, self.i, None)
        msg = self.make_raw(msg)
        self.buf_out |= set(( msg, ))

        # if self.i == self.get_leader(self.round):
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

                votes, evid_set = evidence[item]
                evidence = tuple(evid_set)
                msg = PHASE1LOCK(self.PHASE1LOCK, item, k, evidence, self.i, None)
                msg = self.make_raw(msg)
                self.buf_in.add(msg)
                self.buf_out.add(msg)



    def process_trying_2(self):
        k = self.get_phase_k(self.round)
        for msg in list(self.buf_in):
            if msg.type == self.PHASE1LOCK and msg.phase == k and self.check_phase1msg(msg):
                item = msg.item

                self.locks[item] = msg

                ack = PHASE2ACK(self.PHASE2ACK, item, k, self.i, None)
                ack = self.make_raw(ack)

                self.buf_out.add(ack)

                if self.i == self.get_leader(self.round):
                    self.buf_in.add(ack)


    def process_lockrelease_3(self):
        k = self.get_phase_k(self.round)
        for l in self.locks:
            assert type(self.locks[l]) == PHASE1LOCK

            msg = RELEASE3(self.RELEASE3, self.locks[l], k, self.i, None)
            msg = self.make_raw(msg)

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
        
        if self._trace:
            r = self.round
            p = self.get_phase_k(self.round)
            l = self.get_leader(self.round)

            print("")
            print("-"*20 + " Round %s Phase %s Leader %s" % (r,p,l) + "-"*20)
            print("-"*20 + " Locks " + "-"*20)

            for l, v in self.locks.items():
                print (l,v)

        # Always persist before processing messages.
        self.persist()

        if advance:
            self.round += 1
        return self.round

    def put_messages(self, msgs):
        """ Insert a set of messages into the state machine. """
        if not all([ type(m) in valid_messages for m in msgs]):
            raise Exception("Wrong input type.")

        # Check the senders are well formed:
        for m in msgs:
            assert 0 <= m.sender < self.N

        self.buf_in |= set(msgs)

        if self._trace:
            r = self.round
            p = self.get_phase_k(self.round)
            l = self.get_leader(self.round)

            print("")
            print("-"*20 + " Round %s Phase %s Leader %s" % (r,p,l) + "-"*20)
            print("-"*20 + " Inputs " + "-"*20)

            for m in msgs:
                print("- %s" % str(m))

    def get_messages(self):
        """ Get all the messages emited by the state machine. """
        msgs = self.buf_out.copy()
        self.buf_out.clear()

        if self._trace:
            r = self.round
            p = self.get_phase_k(self.round)
            l = self.get_leader(self.round)

            print("")
            print("-"*20 + " Round %s Phase %s Leader %s" % (r,p,l) + "-"*20)
            print("-"*20 + " Outputs " + "-"*20)

            for m in msgs:
                print("- %s" % str(m))

        return msgs


