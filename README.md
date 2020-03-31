# transaction-database
Transaction Processing and Distributed Data Management

## benchmark
A concurrent transaction simulator that mimics a real life transaction workload of an IoT based system on a database system is build in this folder. 
The simulator is able to create and process multithreaded transactions on multiple database systems, which are MySQL and PostgreSQL here. Running over all data provided comes from [TIPPERS](https://www.ics.uci.edu/~kobsa/papers/2016-PerCom-TIPPERS-Kobsa.pdf), an IoT system, the simulator gives performance report concerning various isolation levels and concurrency degree on MySQL and PostgreSQL, respectively.

## two-phase commit protocol
Distributed transactions using two-phase commit protocol on top of PostgreSQL.
The distributed system is mimiced by creating multiple PostgreSQL servers and agent instances in Docker containers. There is one coordinator node, which will interact with the client, start the distributed transaction and route the inserts/updates to the corresponding nodes through their respective agents.
