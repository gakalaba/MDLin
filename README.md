Gryff
======


### What is Gryff?


Gryff is a replicated storage system that provides the shared object programming interface. Objects of arbitrary size are
accessed with read, write, and read-modify-write operations. Read and write operations correspond to the simplified get/put
interface of key-value stores and they comprise the bulk of many application workloads. Read-modify-write operations allow
clients to atomically read and modify the value of an object, which enables strong synchronization such as compare-and-swaps
or conditional writes.

### What makes Gryff novel?

Gryff provides its interface with low read tail latency relative to state-of-the-art linearizable replication protocols. It does so by unifying two existing techniques for replicated storage: state machine replication and shared registers. State machine replication is necessary to implement strong synchronization primitives, but it has fundamental limitations that inhibit practical systems from achieving low read tail latency. Shared register protocols, on the other hand, provide a read/write interface with low read tail latency, but are fundamentally too weak to implement strong synchronization. Gryff safely and efficiently unifies these two techniques to achieve the best of both.

### How does Gryff work?

Our [NSDI 2020 paper](https://www.usenix.org/conference/nsdi20/presentation/burke) describes the motivation, design, implementation, and evaluation of Gryff.

### What is in this repository?

This repository contains the Go implementations of:

* Gryff
* ABD
* EPaxos
* (classic) MultiPaxos
* Mencius
* Generalized Paxos

The implementations of EPaxos, MultiPaxos, Mencius, and Generalized Paxos were created by Iulian Moraru, David G. Andersen, and Michael Kaminsky as part of the [EPaxos project](https://github.com/efficient/epaxos).

This repository also contains the experimental scripts and configuration used in our NSDI 2020 paper. The experiments may be run on CloudLab using these scripts. A more detailed explanation of how to run these experiments is coming soon!

AUTHORS:

Matthew Burke -- Cornell University

Audrey Cheng, Wyatt Lloyd -- Princeton University
