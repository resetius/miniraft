import asyncio
import sys
from node import *
from fsm import *

async def main():
    nodes = {}
    myid = int(sys.argv[1])
    servers = [(1,"::1",8888),(2,"::1",8889),(3,"::1",8890)]
    for (id,host,port) in servers:
        if id == myid:
            myhost,myport = host,port
        else:
            nodes[id] = Node(id, host, port)

    fsm = FSM(myid, nodes)

    asyncio.create_task(fsm.connector())
    asyncio.create_task(fsm.idle())
    server = await asyncio.start_server(fsm.handle_request, myhost, myport)
    await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
