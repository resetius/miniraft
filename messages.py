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

@dataclass
class AppendEntriesResponse:
    term: int
    success: bool

class Timeout:
    Election = timedelta(seconds = 5)
    Heartbeat = timedelta(seconds = 2)

def serialize(data):
    payload = pickle.dumps(data)
    header = struct.pack('i', len(payload))
    return header,payload
