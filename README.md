# Raft

This is a simple Raft implementation using Python Programming Language.
For the implementation I use the reference raft paper: https://raft.github.io/raft.pdf

Usage:
```
$ python3 main.py 1 # start node 1
$ python3 main.py 2 # start node 2
$ python3 main.py 3 # start node 3
...
$ python3 client.py # start client
> type smth
... # see messages replication in nodes logs
```
