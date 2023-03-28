import pickle
import struct
from dataclasses import dataclass
from datetime import *

@dataclass
class RequestVoteRequest:
    term: int
    candidateId: int
    lastLogIndex: int
    lastLogTerm: int

@dataclass
class RequestVoteResponse:
    term: int
    voteGranted: bool

@dataclass
class AppendEntriesRequest:
    term: int
    leaderId: int
    prevLogIndex: int
    prevLogTerm: int
    leaderCommit: int
    entries = []

@dataclass
class AppendEntriesResponse:
    term: int
    nodeId: int
    success: bool
    matchIndex: int # missing field in raft.pdf

class Timeout:
    Election = timedelta(seconds = 5)
    Heartbeat = timedelta(seconds = 2)

def serialize(data):
    payload = pickle.dumps(data)
    header = struct.pack('i', len(payload))
    return header,payload
