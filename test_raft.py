import unittest
import datetime
from raft import *
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
    def _raft(self, on_send=None, count=3):
        ts = FakeTimeSource(datetime.now())
        nodes = {}
        for i in range(2,count+1):
            nodes[i] = FakeNode(on_send)
        raft = Raft(1, nodes, ts)
        return raft

    def test_initial(self):
        raft = self._raft()
        self.assertEqual(raft.state_func, raft.follower)

    def test_become(self):
        raft = self._raft()
        self.assertEqual(raft.state_func, raft.follower)
        raft.become(raft.candidate)
        self.assertEqual(raft.state_func, raft.candidate)

    def test_become_same_func(self):
        raft = self._raft()
        self.assertEqual(raft.state_func, raft.follower)
        ts = raft.ts
        ts.advance(timedelta(seconds=10))
        raft.become(raft.follower)
        self.assertEqual(raft.state_func, raft.follower)

    def test_apply_empty_result(self):
        raft = self._raft()
        state = raft.state
        volatile_state = raft.volatile_state
        self.assertEqual(raft.state_func, raft.follower)
        raft.apply_result(datetime.now(), None)
        self.assertEqual(raft.state_func, raft.follower)
        self.assertEqual(raft.state, state)
        self.assertEqual(raft.volatile_state, volatile_state)

    def test_apply_state_func_change_result(self):
        raft = self._raft()
        state = raft.state
        volatile_state = raft.volatile_state
        self.assertEqual(raft.state_func, raft.follower)
        raft.apply_result(datetime.now(), Result(
            next_state_func=raft.candidate
        ))
        self.assertEqual(raft.state_func, raft.candidate)
        self.assertEqual(raft.state, state)
        self.assertEqual(raft.volatile_state, volatile_state)

    def test_apply_time_change_result(self):
        raft = self._raft()
        n = datetime.now()
        raft.apply_result(n, Result(
            update_last_time=True
        ))
        self.assertEqual(raft.last_time, n)

    def test_follower_to_candidate_on_timeout(self):
        raft = self._raft()
        ts = raft.ts
        self.assertEqual(raft.state_func, raft.follower)
        raft.process(Timeout())
        self.assertEqual(raft.state_func, raft.follower)
        ts.advance(timedelta(seconds=10))
        raft.process(Timeout())
        self.assertEqual(raft.state_func, raft.candidate)

    def test_follower_append_entries_small_term(self):
        messages=[]
        on_send = lambda y: messages.append(y)

        raft = self._raft(on_send)
        raft.process(AppendEntriesRequest(
            src=2,
            dst=1,
            term=0,
            leaderId=2,
            prevLogIndex=0,
            prevLogTerm=0,
            leaderCommit=0
        ))
        self.assertEqual(messages[0].dst, 2)
        self.assertEqual(messages[0].success, False)

    def _mklog(self, terms):
        entries=[]
        for i in terms:
            entries.append(LogEntry(i, ""))
        return entries

    def test_follower_append_entries_7a(self):
        # leader: 1,1,1,4,4,5,5,6,6,6
        messages=[]
        on_send = lambda y: messages.append(y)

        raft=self._raft(on_send)
        raft.state=State(currentTerm=1,votedFor=2,log=self._mklog([1,1,1,4,4,5,5,6,6]))
        raft.process(AppendEntriesRequest(
            src=2,
            dst=1,
            term=1,
            leaderId=2,
            prevLogIndex=9,
            prevLogTerm=6,
            leaderCommit=9,
            entries=self._mklog([6])
        ))
        self.assertEqual(messages[-1].success, True)
        self.assertEqual(messages[-1].matchIndex, 10)
        self.assertEqual(len(raft.state.log),10)

    def test_follower_append_entries_7b(self):
        # leader: 1,1,1,4,4,5,5,6,6,6
        messages=[]
        on_send = lambda y: messages.append(y)

        raft=self._raft(on_send)
        raft.state=State(currentTerm=1,votedFor=2,log=self._mklog([1,1,1,4]))
        raft.process(AppendEntriesRequest(
            src=2,
            dst=1,
            term=1,
            leaderId=2,
            prevLogIndex=4,
            prevLogTerm=4,
            leaderCommit=9,
            entries=self._mklog([4,5,5,6,6,6])
        ))
        self.assertEqual(messages[-1].success, True)
        self.assertEqual(messages[-1].matchIndex, 10)
        self.assertEqual(len(raft.state.log),10)

    def test_follower_append_entries_7c(self):
        # leader: 1,1,1,4,4,5,5,6,6,6
        messages=[]
        on_send = lambda y: messages.append(y)

        raft=self._raft(on_send)
        raft.state=State(currentTerm=1,votedFor=2,log=self._mklog([1,1,1,4,4,5,5,6,6,6,6]))
        raft.process(AppendEntriesRequest(
            src=2,
            dst=1,
            term=1,
            leaderId=2,
            prevLogIndex=9,
            prevLogTerm=6,
            leaderCommit=9,
            entries=self._mklog([6])
        ))
        self.assertEqual(messages[-1].success, True)
        self.assertEqual(messages[-1].matchIndex, 10)
        self.assertEqual(len(raft.state.log),11)

    def test_follower_append_entries_7f(self):
        # leader: 1,1,1,4,4,5,5,6,6,6
        messages=[]
        on_send = lambda y: messages.append(y)

        raft=self._raft(on_send)
        raft.state=State(currentTerm=1,votedFor=2,log=self._mklog([1,1,1,2,2,2,3,3,3,3,3]))
        raft.process(AppendEntriesRequest(
            src=2,
            dst=1,
            term=8,
            leaderId=2,
            prevLogIndex=3,
            prevLogTerm=1,
            leaderCommit=9,
            entries=self._mklog([4,4,5,5,6,6,6])
        ))
        self.assertEqual(messages[-1].success, True)
        self.assertEqual(messages[-1].matchIndex, 10)
        self.assertEqual(len(raft.state.log),10)
        self.assertEqual(raft.state.log,self._mklog([1,1,1,4,4,5,5,6,6,6]))

    def test_follower_append_entries_empty_to_empty_log(self):
        messages=[]
        on_send = lambda y: messages.append(y)

        raft = self._raft(on_send)
        raft.process(AppendEntriesRequest(
            src=2,
            dst=1,
            term=1,
            leaderId=2,
            prevLogIndex=0,
            prevLogTerm=0,
            leaderCommit=0
        ))
        self.assertEqual(messages[0].dst, 2)
        self.assertEqual(messages[0].success, True)
        self.assertEqual(messages[0].matchIndex, 0)

    def test_candidate_initiate_election(self):
        messages=[]
        on_send = lambda y: messages.append(y)

        raft = self._raft(on_send)
        raft.ts.advance(timedelta(seconds=10))
        term = raft.state.currentTerm
        raft.become(raft.candidate)
        self.assertEqual(term+1, raft.state.currentTerm) # update term
        self.assertEqual(raft.ts.now(), raft.last_time) # update last time
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0], RequestVoteRequest(1,0,term+1, raft.id, 0, 0))
        self.assertEqual(messages[1], RequestVoteRequest(1,0,term+1, raft.id, 0, 0))

    def test_candidate_vote_request_small_term(self):
        raft = self._raft()
        ts=raft.ts
        result = raft.candidate(ts.now(),ts.now(),RequestVoteRequest(2, 1, 0, 2, 1, 1), raft.state, raft.volatile_state)
        self.assertEqual(result.message, RequestVoteResponse(1,2,raft.state.currentTerm, False))

    def test_candidate_vote_request_ok_term(self):
        raft = self._raft()
        ts=raft.ts
        result = raft.candidate(ts.now(),ts.now(),RequestVoteRequest(2, 1, 1, 2, 1, 1), raft.state, raft.volatile_state)
        self.assertEqual(result.message, RequestVoteResponse(1, 2, raft.state.currentTerm, True))
        self.assertEqual(raft.state.currentTerm, 1)

    def test_candidate_vote_request_big(self):
        raft = self._raft()
        ts=raft.ts
        raft.become(raft.candidate)
        result = raft.process(RequestVoteRequest(2, 1, 3, 2, 1, 1))
        self.assertEqual(raft.state_func, raft.follower)

    def test_candidate_vote_after_start(self):
        messages = []
        on_send = lambda y: messages.append(y)
        raft = self._raft(on_send)
        self.assertEqual(raft.state_func, raft.follower)
        raft.ts.advance(timedelta(seconds=10))
        raft.become(raft.candidate) # initiates election
        self.assertEqual(raft.state.votedFor, 1)
        self.assertEqual(raft.state.currentTerm, 2)
        raft.process(RequestVoteRequest(2,1,2, 2, 1, 1))
        self.assertEqual(messages[-1].voteGranted, False)

        # request with higher term => follower
        raft.process(RequestVoteRequest(2,1,3, 3, 1, 1))
        self.assertEqual(raft.state.votedFor, 3)
        self.assertEqual(messages[-1].voteGranted, True)

    def test_election_5_nodes(self):
        raft = self._raft(None, 5)
        raft.ts.advance(timedelta(seconds=10))
        raft.become(raft.candidate)
        raft.process(RequestVoteResponse(src=2,dst=1,term=2, voteGranted=True))

        self.assertEqual(raft.state_func, raft.candidate)
        raft.process(RequestVoteResponse(src=2,dst=1,term=2, voteGranted=True))
        self.assertEqual(raft.state_func, raft.candidate)

        raft.process(RequestVoteResponse(src=3,dst=1,term=2, voteGranted=True))
        self.assertEqual(raft.state_func, raft.leader)

    def test_commit_advance(self):
        state=State(currentTerm=1,log=self._mklog([1,1,1,1]))
        s = VolatileState(matchIndex={1:1})
        s1 = s.with_commit_advance(3,1,state)
        self.assertEqual(s1.commitIndex, 1)
        s1 = s.with_commit_advance(5,1,state)
        self.assertEqual(s1.commitIndex, 0)

        s = VolatileState(matchIndex={1:1,2:2})
        s1 = s.with_commit_advance(3,2,state)
        self.assertEqual(s1.commitIndex, 2)
        s1 = s.with_commit_advance(5,2,state)
        self.assertEqual(s1.commitIndex, 1)

    def test_commit_advance_wrong_term(self):
        state=State(currentTerm=2,log=self._mklog([1,1,1,1]))
        s = VolatileState(matchIndex={1:1,2:2})
        s1 = s.with_commit_advance(3,2,state)
        self.assertEqual(s1.commitIndex, 0)

if __name__ == "__main__":
    unittest.main()
