import pickle
import struct
from dataclasses import dataclass, field
from datetime import *
from typing import *

@dataclass(frozen=True)
class LogEntry:
    term: int = 1
    data: bytes = None

@dataclass
class RequestVoteRequest:
    src: int
    dst: int
    term: int
    candidateId: int
    lastLogIndex: int
    lastLogTerm: int

@dataclass
class RequestVoteResponse:
    src: int
    dst: int
    term: int
    voteGranted: bool

@dataclass
class AppendEntriesRequest:
    src: int
    dst: int
    term: int
    leaderId: int
    prevLogIndex: int
    prevLogTerm: int
    leaderCommit: int
    entries: List[LogEntry] = field(default_factory=list)

@dataclass
class AppendEntriesResponse:
    src: int
    dst: int
    term: int
    success: bool
    matchIndex: int # missing field in raft.pdf

@dataclass
class CommandRequest:
    data: bytes

@dataclass
class CommandResponse:
    pass

class Timeout:
    Election = timedelta(seconds = 5)
    Heartbeat = timedelta(seconds = 2)

def serialize(data):
    payload = pickle.dumps(data)
    header = struct.pack('i', len(payload))
    return header,payload
