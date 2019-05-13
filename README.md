<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Tong's fork of MIT-6.824](#tongs-fork-of-mit-6824)
  - [Lab2: raft](#lab2-raft)
    - [Part 2A](#part-2a)
    - [Part 2B](#part-2b)
    - [Part 2C](#part-2c)
    - [Notes](#notes)
      - [Term](#term)
    - [How to commit and persist efficiently ?](#how-to-commit-and-persist-efficiently-)
  - [Lab3: kvraft](#lab3-kvraft)
    - [Notes](#notes-1)
      - [Linearizability](#linearizability)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Tong's fork of [MIT-6.824](https://github.com/chaozh/MIT-6.824)

## [Lab2: raft](https://pdos.csail.mit.edu/6.824/schedule.html)

### [Part 2A](https://github.com/caitong93/MIT-6.824/tree/2A)

### [Part 2B](https://github.com/caitong93/MIT-6.824/tree/2B)

### [Part 2C](https://github.com/caitong93/MIT-6.824/tree/2C)

### Notes

#### Term

From section 5.1

>Terms act as a logical clock in Raft, and they allow servers to detect obsolete information such as stale leaders. Each server stores a current term number, which increases monotonically over time. Current terms are exchanged whenever servers communicate; if one server's current term is smaller than the other's, then it updates its current term to the larger value. If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state. If a server receives a request with a stale term number, it rejects the request.

### How to commit and persist efficiently ?

TODO

## [Lab3: kvraft](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html)

### Notes

#### Linearizability

Clients perform concurrent Put and Get operations? How to know the results are correct given operation histories?
Here comes linearizability, a correctness condition for concurrent objects. If histories are linearizable, they are correct.

Get started:
- https://pdos.csail.mit.edu/6.824/notes/l-spinnaker.txt
- https://cs.brown.edu//~mph/HerlihyW90/p463-herlihy.pdf

Futher readings:
- http://loopjump.com/distributed_consistency_model/
- https://github.com/etcd-io/etcd/issues/741