import asyncio
import messages
import struct
import pickle
from dataclasses import dataclass

@dataclass
class Config:
    id: int
    host: str
    replication_port: int
    port: int

class Sender:
    def __init__(self, writer):
        self.writer = writer

    def send(self, data):
        header, payload = messages.serialize(data)
        self.writer.write(header)
        self.writer.write(payload)

class Receiver:
    def __init__(self, reader):
        self.reader = reader

    async def rcv(self):
        header = await self.reader.read(4)
        size = struct.unpack('i', header)[0]
        payload = await self.reader.read(size)
        obj = pickle.loads(payload)
        return obj

class Node:
    def __init__(self, id, host, port):
        self.id = id
        self.connected = False
        self.host = host
        self.port = port
        self.connect_task = None
        self.io_task = None
        self.writer = None
        self.reader = None
        self.handler = None

    def start(self, handler):
        self.handler = handler
        self._reconnect()

    def is_connected(self):
        return self.connected

    def send(self, data):
        if self.is_connected():
            print("Send %s to %d"%(data, self.id))
            self.sender.send(data)

    async def rcv(self):
        return await self.receiver.rcv()

    def _reconnect(self):
        if self.writer:
            self.writer.close()
        self.connected = False
        if not self.connect_task:
            self.connect_task = asyncio.create_task(self.connect())

    async def drain(self):
        try:
            if self.writer:
                await self.writer.drain()
        except:
            self._reconnect()

    async def connect(self):
        while not self.connected:
            try:
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
                self.sender = Sender(self.writer)
                self.receiver = Receiver(self.reader)
                print("Connected [%s,%d]"%(self.host,self.port))
                self.connected = True
                if self.io_task:
                    self.io_task.cancel()
                if self.handler:
                    self.io_task = asyncio.create_task(self.handler(self.reader, self.writer))
            except Exception as ex:
                print("[%s:%d]: Retry in 5 secs: %s"%(self.host,self.port,ex))
                await asyncio.sleep(5)
        self.connect_task = None
