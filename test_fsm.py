import unittest
import datetime
from fsm import *
from timesource import *

class FakeNode:
    def __init__(self, on_send=None):
        self.on_send=on_send
        if self.on_send is None:
            self.on_send = self._on_send

    def start(self, unused):
        pass

    def _on_send(self, message):
        pass

    def send(self, message):
        return self.on_send(message)

class Test(unittest.TestCase):
    def _fsm(self, on_send=None, count=3):
        ts = FakeTimeSource(datetime.now())
        nodes = {}
        for i in range(2,count+1):
            nodes[i] = FakeNode(on_send)
        fsm = FSM(1, nodes, ts)
        return fsm

    def test_initial(self):
        fsm = self._fsm()
        self.assertEqual(fsm.state_func, fsm.follower)

    def test_become(self):
        fsm = self._fsm()
        self.assertEqual(fsm.state_func, fsm.follower)
        fsm.become(fsm.candidate)
        self.assertEqual(fsm.state_func, fsm.candidate)

    def test_become_same_func(self):
        fsm = self._fsm()
        self.assertEqual(fsm.state_func, fsm.follower)
        ts = fsm.ts
        ts.advance(timedelta(seconds=10))
        fsm.become(fsm.follower)
        self.assertEqual(fsm.state_func, fsm.follower)

    def test_apply_empty_result(self):
        fsm = self._fsm()
        state = fsm.state
        volatile_state = fsm.volatile_state
        self.assertEqual(fsm.state_func, fsm.follower)
        fsm.apply_result(datetime.now(), None)
        self.assertEqual(fsm.state_func, fsm.follower)
        self.assertEqual(fsm.state, state)
        self.assertEqual(fsm.volatile_state, volatile_state)

    def test_apply_state_func_change_result(self):
        fsm = self._fsm()
        state = fsm.state
        volatile_state = fsm.volatile_state
        self.assertEqual(fsm.state_func, fsm.follower)
        fsm.apply_result(datetime.now(), Result(
            next_state_func=fsm.candidate
        ))
        self.assertEqual(fsm.state_func, fsm.candidate)
        self.assertEqual(fsm.state, state)
        self.assertEqual(fsm.volatile_state, volatile_state)

    def test_apply_time_change_result(self):
        fsm = self._fsm()
        n = datetime.now()
        fsm.apply_result(n, Result(
            update_last_time=True
        ))
        self.assertEqual(fsm.last_time, n)

    def test_follower_to_candidate_on_timeout(self):
        fsm = self._fsm()
        ts = fsm.ts
        self.assertEqual(fsm.state_func, fsm.follower)
        fsm.process(Timeout())
        self.assertEqual(fsm.state_func, fsm.follower)
        ts.advance(timedelta(seconds=10))
        fsm.process(Timeout())
        self.assertEqual(fsm.state_func, fsm.candidate)

    def test_follower_append_entries(self):
        # TODO: implement
        fsm = self._fsm()
        fsm.process(AppendEntriesRequest(term=1))

    def test_candidate_initiate_election(self):
        messages=[]
        on_send = lambda y: messages.append(y)

        fsm = self._fsm(on_send)
        fsm.ts.advance(timedelta(seconds=10))
        term = fsm.state.currentTerm
        fsm.become(fsm.candidate)
        self.assertEqual(term+1, fsm.state.currentTerm) # update term
        self.assertEqual(fsm.ts.now(), fsm.last_time) # update last time
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0], RequestVoteRequest(term+1, fsm.id, 1, 1))
        self.assertEqual(messages[1], RequestVoteRequest(term+1, fsm.id, 1, 1))

    def test_candidate_vote_request_small_term(self):
        fsm = self._fsm()
        ts=fsm.ts
        result = fsm.candidate(ts.now(),ts.now(),RequestVoteRequest(0, 2, 1, 1), fsm.state, fsm.volatile_state)
        self.assertEqual(result.message, RequestVoteResponse(fsm.state.currentTerm, False))
        self.assertEqual(result.recepient, 2)

    def test_candidate_vote_request_ok_term(self):
        fsm = self._fsm()
        ts=fsm.ts
        result = fsm.candidate(ts.now(),ts.now(),RequestVoteRequest(1, 2, 1, 1), fsm.state, fsm.volatile_state)
        self.assertEqual(result.message, RequestVoteResponse(fsm.state.currentTerm, True))
        self.assertEqual(result.recepient, 2)
        self.assertEqual(fsm.state.currentTerm, 1)

    def test_candidate_vote_after_start(self):
        messages = []
        on_send = lambda y: messages.append(y)
        fsm = self._fsm(on_send)
        self.assertEqual(fsm.state_func, fsm.follower)
        fsm.ts.advance(timedelta(seconds=10))
        fsm.become(fsm.candidate) # initiates election
        self.assertEqual(fsm.state.votedFor, 1)
        self.assertEqual(fsm.state.currentTerm, 2)
        fsm.process(RequestVoteRequest(2, 2, 1, 1))
        self.assertEqual(messages[-1].voteGranted, False)

        # request with higher term => follower
        fsm.process(RequestVoteRequest(3, 3, 1, 1))
        self.assertEqual(fsm.state.votedFor, 3)
        self.assertEqual(messages[-1].voteGranted, True)

    def test_election_5_nodes(self):
        fsm = self._fsm(None, 5)
        fsm.ts.advance(timedelta(seconds=10))
        fsm.become(fsm.candidate)
        fsm.process(RequestVoteResponse(2, True))
        self.assertEqual(fsm.state_func, fsm.candidate)
        fsm.process(RequestVoteResponse(2, True))
        self.assertEqual(fsm.state_func, fsm.leader)

if __name__ == "__main__":
    unittest.main()
