import asyncio
from messages import *
from node import *

async def handler(reader, writer):
    pass

async def main():
    servers = [(1,"::1",8888),(2,"::1",8889),(3,"::1",8890)]
    i=0
    node = Node(servers[i][0], servers[i][1], servers[i][2])
    await node.connect()
    node.send(
        CommandRequest(0)
    )
    await node.drain()


if __name__ == "__main__":
    asyncio.run(main())

