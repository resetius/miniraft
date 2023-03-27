import pickle
import struct
import datetime
import asyncio
from dataclasses import dataclass
from messages import *
from typing import *
from timesource import *

@dataclass(frozen=True)
class LogEntry:
    term: int = 1

@dataclass(frozen=True,init=True)
class State:
    currentTerm: int = 1
    votedFor: int = 0
    log = [LogEntry()]

@dataclass(frozen=True,init=True)
class VolatileState:
    commitIndex: int = 0
    lastApplied: int = 0
    nextIndex = {}
    matchIndex = {}
    votes: int = 0

@dataclass(frozen=True,init=True)
class Result:
    next_state: Any = None
    next_volatile_state: Any = None
    next_state_func: Any = None
    update_last_time: bool = False
    message: Any = None
    recepient: int = 0

class FSM:
    def __init__(self, id: int, nodes, ts: TimeSource = TimeSource()):
        self.id = id
        self.nodes = nodes
        self.ts = ts
        self.min_votes = (len(nodes)+len(nodes)+1)//2
        self.state = State()
        self.volatile_state = VolatileState()
        self.state_func = self.follower
        self.last_time = self.ts.now()

        for k,v in self.nodes.items():
            v.start(self.handle_request)

    def on_append_entries(self, message: AppendEntriesRequest, state: State):
        if message.term < state.currentTerm:
            return Result(
                message=AppendEntriesResponse(state.currentTerm, success=False),
                update_last_time=True
            )
        # TODO: Reply false if log doesn’t contain an entry at prevLogIndex
        # whose term matches prevLogTerm (§5.3)
        # TODO: If an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it (§5.3)
        # TODO: Append any new entries not already in the log
        # TODO: If leaderCommit > commitIndex, set commitIndex =
        # min(leaderCommit, index of last new entry)
        return Result(
            next_state=State(currentTerm=message.term, votedFor=state.votedFor),
            message=AppendEntriesResponse(message.term, success=True),
            next_state_func=self.follower,
            update_last_time=True
        )

    def on_request_vote(self, message: RequestVoteRequest, state: State, volatile_state: VolatileState):
        if message.term < state.currentTerm:            
            return Result(                
                message=RequestVoteResponse(state.currentTerm, False),
                recepient=message.candidateId,
            )
        elif message.term == state.currentTerm:
            accept=False
            if state.votedFor == 0:                
                accept=True
            elif state.votedFor == message.candidateId and message.lastLogTerm > state.log[-1].term:
                accept=True
            elif state.votedFor == message.candidateId and message.lastLogTerm == state.log[-1].term and message.lastLogIndex >= len(state.log):
                accept=True

            return Result(
                next_state=State(currentTerm=message.term, votedFor=message.candidateId),
                message=RequestVoteResponse(message.term, accept),
                recepient=message.candidateId,
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
            return self.on_append_entries(message, state)

        return None

    def candidate(self, now: datetime, last: datetime, message, state: State, volatile_state: VolatileState) -> Result:
        if isinstance(message, Timeout):
            if (now - last > Timeout.Election):
                return Result(
                    next_state=State(currentTerm=state.currentTerm+1,votedFor=self.id),
                    next_volatile_state=VolatileState(votes=1),
                    update_last_time=True,
                    message=RequestVoteRequest(state.currentTerm+1, self.id, 1,1),
                    recepient=-1
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
                return Result(
                    next_state=State(currentTerm=state.currentTerm,votedFor=state.votedFor),
                    next_volatile_state=VolatileState(votes=votes),
                    next_state_func=self.leader,
                    update_last_time=True
                )
            return Result(
                next_state=State(currentTerm=state.currentTerm,votedFor=state.votedFor),
                next_volatile_state=VolatileState(votes=votes),
            )
        elif isinstance(message, AppendEntriesRequest):
            return self.on_append_entries(message, state)
        else:
            pass

        return None

    def leader(self, now: datetime, last: datetime, message, state: State, volatile_state: VolatileState) -> Result:
        if isinstance(message, Timeout):
            if (now - last) > Timeout.Heartbeat:
                return Result(
                    update_last_time=True,
                    recepient=-1,
                    message=AppendEntriesRequest(state.currentTerm)
                )
        elif isinstance(message, AppendEntriesResponse):
            # TODO: Response from follower
            pass
        elif isinstance(message, AppendEntriesRequest):
            # Bad request
            pass
        elif isinstance(message, RequestVoteRequest):
            return self.on_request_vote(message, state, volatile_state)
        elif isinstance(message, RequestVoteResponse):
            # Bad request
            pass

        return None

    def become(self, state_func):
        if self.state_func != state_func:
            print("State change %s->%s"%(self.state_func, state_func))
            self.state_func = state_func
            self.process(Timeout(), None)

    def process(self, message, sock=None):
        now = self.ts.now()
        if not isinstance(message,Timeout) and message.term > self.state.currentTerm:
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
                if result.recepient == -1:
                    for k,v in self.nodes.items():
                        v.send(result.message)
                elif result.recepient == 0:
                    if sock:
                        header, payload = serialize(result.message)
                        sock.write(header)
                        sock.write(payload)
                else:
                    self.nodes[result.recepient].send(result.message)
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
        while True:
            self.process(Timeout())
            await asyncio.sleep(0.01)
