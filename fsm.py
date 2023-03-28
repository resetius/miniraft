import pickle
import struct
import datetime
import asyncio
from collections import defaultdict
from dataclasses import dataclass,field
from messages import *
from typing import *
from timesource import *

@dataclass(frozen=True,init=True)
class State:
    currentTerm: int = 1
    votedFor: int = 0
    log: List[LogEntry] = field(default_factory=list)

@dataclass(frozen=True,init=True)
class VolatileState:
    commitIndex: int = 0
    lastApplied: int = 0
    nextIndex: Dict[int,int] = field(default_factory=lambda: defaultdict(int))
    matchIndex: Dict[int,int] = field(default_factory=lambda: defaultdict(int))
    votes: int = 0

    def with_set_vote(self, vote):
        return VolatileState(commitIndex=self.commitIndex, lastApplied=self.lastApplied, nextIndex=self.nextIndex, matchIndex=self.matchIndex, votes=vote)

    def with_last_applied(self, index):
        return VolatileState(commitIndex=self.commitIndex, lastApplied=index, nextIndex=self.nextIndex, matchIndex=self.matchIndex, votes=self.votes)

    def with_commit_advance(self, nservers, lastIndex):
        indices = list(self.matchIndex.values())
        indices.append(lastIndex)
        while len(indices)<nservers:
            indices.append(0)
        indices.sort()
        commitIndex=max(self.commitIndex, indices[nservers//2])
        return VolatileState(commitIndex=commitIndex, lastApplied=self.lastApplied, nextIndex=self.nextIndex, matchIndex=self.matchIndex, votes=self.votes)

    def with_commit_index(self, index):
        return VolatileState(commitIndex=index, lastApplied=self.lastApplied, nextIndex=self.nextIndex, matchIndex=self.matchIndex, votes=self.votes)

    def with_vote(self):
        return VolatileState(commitIndex=self.commitIndex, lastApplied=self.lastApplied, nextIndex=self.nextIndex, matchIndex=self.matchIndex, votes=self.votes+1)

    def with_next_index(self, d):
        return VolatileState(commitIndex=self.commitIndex, lastApplied=self.lastApplied, nextIndex=self.nextIndex|d, matchIndex=self.matchIndex, votes=self.votes)

    def with_match_index(self, d):
        return VolatileState(commitIndex=self.commitIndex, lastApplied=self.lastApplied, nextIndex=self.nextIndex, matchIndex=self.matchIndex|d, votes=self.votes)

@dataclass(frozen=True,init=True)
class Result:
    next_state: Any = None
    next_volatile_state: Any = None
    next_state_func: Any = None
    update_last_time: bool = False
    message: Any = None
    messages: Any = None

class FSM:
    def __init__(self, id: int, nodes, ts: TimeSource = TimeSource()):
        self.id = id
        self.nodes = nodes
        self.ts = ts
        self.min_votes = (len(nodes)+2)//2
        self.npeers = len(nodes)
        self.nservers = len(nodes)+1
        assert(self.npeers % 2 == 0)
        assert(self.nservers % 2 == 1)
        self.state = State()
        self.volatile_state = VolatileState()
        self.state_func = self.follower
        self.last_time = self.ts.now()

        for k,v in self.nodes.items():
            v.start(self.handle_request)

    def on_append_entries(self, message: AppendEntriesRequest, state: State, volatile_state: VolatileState):

        if message.term < state.currentTerm:
            return Result(
                message=AppendEntriesResponse(src=self.id, dst=message.src, term=state.currentTerm, success=False, nodeId=self.id, matchIndex=0),
                update_last_time=True
            )

        assert(message.term == state.currentTerm)

        matchIndex=0
        commitIndex=volatile_state.commitIndex
        success=False
        if (message.prevLogIndex==0 or (message.prevLogIndex <= len(state.log) and self._log_term(state, message.prevLogIndex)==message.prevLogTerm)):
            # append
            success=True
            index=message.prevLogIndex
            log=state.log
            for entry in message.entries:
                index=index+1
                # replace or append log entries
                if self._log_term(state, index) != entry.term:
                    while len(log) > index-1:
                        log.pop()
                    log.append(entry)

            matchIndex=index
            commitIndex=max(commitIndex, message.leaderCommit)

        return Result(
            message=AppendEntriesResponse(src=self.id, dst=message.src, term=message.term, success=success, matchIndex=matchIndex),
            next_volatile_state=volatile_state.with_commit_index(commitIndex),
            next_state_func=self.follower,
            update_last_time=True
        )

    def on_request_vote(self, message: RequestVoteRequest, state: State, volatile_state: VolatileState):
        if message.term < state.currentTerm:
            return Result(
                message=RequestVoteResponse(src=self.id, dst=message.src, term=state.currentTerm, voteGranted=False)
            )
        elif message.term == state.currentTerm:
            accept=False
            if state.votedFor == 0:
                accept=True
            elif state.votedFor == message.candidateId and message.lastLogTerm > self._log_term(state):
                accept=True
            elif state.votedFor == message.candidateId and message.lastLogTerm == self._log_term(state) and message.lastLogIndex >= len(state.log):
                accept=True

            return Result(
                next_state=State(currentTerm=message.term, votedFor=message.candidateId),
                message=RequestVoteResponse(src=self.id, dst=message.src, term=message.term, voteGranted=accept)
            )

    def _log_term(self, state: State, index: int = -1):
        if index < 0: index = len(state.log)
        if index < 1 or index > len(state.log):
            return 0
        else:
            return state.log[index-1].term

    def _create_vote(self, state):
        return RequestVoteRequest(
            src=self.id,
            dst=0,
            term=state.currentTerm+1,
            candidateId=self.id,
            lastLogIndex=len(state.log),
            lastLogTerm=0 if len(state.log)==0 else state.log[-1].term
        )

    def _create_append_entries(self, state, volatile_state, nodeId):
        prevIndex = volatile_state.nextIndex[nodeId] - 1;
        lastIndex = min(prevIndex+1,len(state.log))
        if volatile_state.matchIndex[nodeId]+1 < volatile_state.nextIndex[nodeId]:
            lastIndex = prevIndex

        return AppendEntriesRequest(
            src=self.id,
            dst=nodeId,
            term=state.currentTerm,
            leaderId=self.id,
            prevLogIndex=prevIndex,
            prevLogTerm=self._log_term(state, prevIndex),
            entries=state.log[volatile_state.commitIndex:lastIndex],
            leaderCommit=volatile_state.commitIndex
        )

    def follower(self, now: datetime, last: datetime, message, state: State, volatile_state: VolatileState) -> Result:
        if isinstance(message, Timeout):
            if (now - last > Timeout.Election):
                return Result(
                    next_state_func=self.candidate,
                    update_last_time=True
                )
        elif isinstance(message, RequestVoteRequest):
            return self.on_request_vote(message, state, volatile_state)
        elif isinstance(message, AppendEntriesRequest):
            return self.on_append_entries(message, state, volatile_state)

        return None

    def candidate(self, now: datetime, last: datetime, message, state: State, volatile_state: VolatileState) -> Result:
        if isinstance(message, Timeout):
            if (now - last > Timeout.Election):
                return Result(
                    next_state=State(currentTerm=state.currentTerm+1,votedFor=self.id),
                    next_volatile_state=volatile_state.with_set_vote(1),
                    update_last_time=True,
                    message=self._create_vote(state)
                )
        elif isinstance(message, RequestVoteRequest):
            return self.on_request_vote(message, state, volatile_state)
        elif isinstance(message, RequestVoteResponse):
            votes = volatile_state.votes
            if message.term > state.currentTerm:
                return Result(
                    next_state=State(currentTerm=state.currentTerm,votedFor=state.votedFor),
                    next_state_func=self.follower,
                    update_last_time=True
                )
            if message.voteGranted and message.term == state.currentTerm:
                votes = votes+1
            print("Need/total %d/%d"%(self.min_votes,votes))
            if votes >= self.min_votes:
                value = len(state.log)+1
                next_indices = {key: value for key in range(1,len(self.nodes)+2)}
                return Result(
                    next_state=State(currentTerm=state.currentTerm,votedFor=state.votedFor),
                    next_volatile_state=volatile_state.with_set_vote(votes).with_next_index(next_indices),
                    next_state_func=self.leader,
                    update_last_time=True
                )
            return Result(
                next_state=State(currentTerm=state.currentTerm,votedFor=state.votedFor),
                next_volatile_state=volatile_state.with_set_vote(votes),
            )
        elif isinstance(message, AppendEntriesRequest):
            return self.on_append_entries(message, state, volatile_state)

        return None

    def leader(self, now: datetime, last: datetime, message, state: State, volatile_state: VolatileState) -> Result:
        if isinstance(message, Timeout):
            if (now - last) > Timeout.Heartbeat:
                return Result(
                    update_last_time=True,
                    messages=[self._create_append_entries(state, volatile_state, nodeId) for nodeId in self.nodes.keys()]
                )
        elif isinstance(message, AppendEntriesResponse):
            if message.term == state.currentTerm:
                nodeId=message.src
                if message.success:
                    matchIndex = max(volatile_state.matchIndex[nodeId], message.matchIndex)
                    return Result(
                        next_volatile_state=volatile_state.with_match_index({nodeId: matchIndex}).with_next_index({nodeId: message.matchIndex+1}).with_commit_advance(self.nservers,len(state.log))
                    )
                else:
                    return Result(
                        next_volatile_state=volatile_state.with_next_index({nodeId: max(1, volatile_state.nextIndex[nodeId]-1)})
                    )
        elif isinstance(message, CommandRequest):
            # client request
            log=state.log
            log.append(LogEntry(term=state.currentTerm))
            return Result(
                next_state=State(currentTerm=state.currentTerm, votedFor=state.votedFor, log=log),
                next_volatile_state=volatile_state.with_last_applied(len(log)).with_commit_advance(self.nservers,len(log))
            )
        elif isinstance(message, RequestVoteRequest):
            return self.on_request_vote(message, state, volatile_state)
        elif isinstance(message, RequestVoteResponse):
            # skip additional votes
            pass

        return None

    def become(self, state_func):
        if self.state_func != state_func:
            print("State change %s->%s"%(self.state_func, state_func))
            self.state_func = state_func
            self.process(Timeout(), None)

    def process(self, message, sock=None):
        now = self.ts.now()
        if not isinstance(message,Timeout) and not isinstance(message,CommandRequest) and message.term > self.state.currentTerm:
            self.state=State(currentTerm=message.term, votedFor=0)
            self.state_func=self.follower
        self.apply_result(now, self.state_func(now, self.last_time, message, self.state, self.volatile_state), sock)

    def apply_result(self, now, result, sock=None):
        if result:
            if result.update_last_time:
                self.last_time = now
            if result.next_state:
                self.state = result.next_state
            if result.next_volatile_state:
                self.volatile_state = result.next_volatile_state
            if result.message:
                if result.message.dst == 0:
                    for k,v in self.nodes.items():
                        v.send(result.message)
                else:
                    self.nodes[result.message.dst].send(result.message)
            if result.messages:
                for m in result.messages:
                    self.nodes[m.dst].send(m)

            if result.next_state_func:
                self.become(result.next_state_func)

    async def handle_request(self, reader, writer):
        # Handle per connection
        try:
            while True:
                header = await reader.read(4)
                size = struct.unpack('i', header)[0]
                payload = await reader.read(size)
                obj = pickle.loads(payload)
                print("Received: %s"%(obj))
                self.process(obj, writer)
        except Exception as ex:
            print("Exception: %s"%(ex))

    async def connector(self):
        while True:
            for k,v in self.nodes.items():
                await v.drain()
            await asyncio.sleep(0.1)

    async def idle(self):
        t0=datetime.now()
        dt=timedelta(seconds=2)
        while True:
            self.process(Timeout())
            t1=datetime.now()
            if t1>t0+dt:
                print("State: %s %s"%(self.state,self.volatile_state))
                t0=t1
            await asyncio.sleep(0.01)
