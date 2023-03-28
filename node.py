import asyncio
import messages

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

    def start(self, handler):
        self.handler = handler
        self._reconnect()

    def is_connected(self):
        return self.connected

    def send(self, data):
        if self.is_connected():
            print("Send %s to %d"%(data, self.id))
            header, payload = messages.serialize(data)
            self.writer.write(header)
            self.writer.write(payload)

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
                print("Connected [%s,%d]"%(self.host,self.port))
                self.connected = True
                if self.io_task:
                    self.io_task.cancel()
                if self.handler:
                    self.io_task = asyncio.create_task(self.handler(self.reader, self.writer))
            except:
                print("[%s:%d]: Retry in 5 secs"%(self.host,self.port))
                await asyncio.sleep(5)
        self.connect_task = None
