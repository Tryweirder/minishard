Minishard — lightweigth sharding for distributed Erlang applications
=======

Goal
-----
Sometimes you need to store large amount of temporary data available from any node of Erlang cluster.
Storing all the data on single node may cause a memory problem and makes this node a single point of failure.
Replication is even worse sometimes — in case of unreliable network (e.g. multiple datacenters) you get
inconsistencies and merge conflicts.

Minishard keeps configured number of unique shards allocated on nodes of your cluster, restores cluster connectivity,
resolves possible conflicts after netsplit, notifies your application when cluster degrades.

How minishard is supposed to work
-----------
TODO
