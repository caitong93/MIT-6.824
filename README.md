<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Tong's fork of MIT-6.824](#tongs-fork-of-mit-6824)
  - [Lab2: Raft](#lab2-raft)
    - [Part 2A](#part-2a)
    - [Part 2B](#part-2b)
    - [Notes](#notes)
      - [Term](#term)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Tong's fork of [MIT-6.824](https://github.com/chaozh/MIT-6.824)

## [Lab2: Raft](http://nil.csail.mit.edu/6.824/2017/labs/lab-raft.html)

### [Part 2A](https://github.com/caitong93/MIT-6.824/tree/2A)

### [Part 2B](https://github.com/caitong93/MIT-6.824/tree/2B)

### Notes

#### Term

From section 5.1

>Terms act as a logical clock in Raft, and they allow servers to detect obsolete information such as stale leaders. Each server stores a current term number, which increases monotonically over time. Current terms are exchanged whenever servers communicate; if one server's current term is smaller than the other's, then it updates its current term to the larger value. If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state. If a server receives a request with a stale term number, it rejects the request.

