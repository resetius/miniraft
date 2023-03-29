from datetime import datetime,timedelta
import asyncio
from messages import Timeout

class Net:
    def __init__(self, nodes, raft):
        self.nodes = nodes
        self.raft = raft
        for k,v in self.nodes.items():
            v.start(self.handle_request)

    async def handle_request(self, reader, writer):
        # Handle per connection
        try:
            sender = Sender(writer)
            receiver = Receiver(reader)
            while True:
                obj = await receiver.rcv()
                print("Received: %s"%(obj))
                self.raft.process(obj, sender)
                await writer.drain()
        except Exception as ex:
            import traceback
            print("Exception: %s"%(ex))
            traceback.print_exception(ex)

    async def connector(self):
        while True:
            for k,v in self.nodes.items():
                await v.drain()
            await asyncio.sleep(0.1)

    async def idle(self):
        t0=datetime.now()
        dt=timedelta(seconds=2)
        while True:
            self.raft.process(Timeout())
            t1=datetime.now()
            if t1>t0+dt:
                print("State: %s %s"%(self.raft.state,self.raft.volatile_state))
                t0=t1
            await asyncio.sleep(0.01)
