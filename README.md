# Hooray2

Experiments with Datalog.

The goal of this repository is to test some ideas around Datalog.

The two main concepts are Worst-case optimal join (WCOJ) and DBSP a formal framwork for incremental computation.
We also try to combine the two.

## Join Algorithms

Currently the repository implements 2 join algorithms:
- Leapfrog Triejoin ( https://arxiv.org/pdf/1210.0481.pdf )
- Generic Join (a variation of Generic-Join https://arxiv.org/abs/1310.3314 )

Some others that might be interesting to add for comparison.
- Hash Join (just standard binary tree join strategy)
- Yannakakis algorithm (the standard algorithm for non-cyclic queries)
- Something that combines standard binary joins with WCOJ (see Free Join)
- TreeTracker Join (very recent)

## DBSP

DBSP is a formal framework for incremental view maintenance for query languages. See https://arxiv.org/abs/2203.16684
The folks at [Feldera](https://www.feldera.com/) are implementing it for SQL.
The idea here is to do it for a datalog engine with an datomic-like API.
I also couldn't find any mention of combining WCOJ with DBSP.
So a first goal is to make Generic-Join work with DBSP and do so in a WCO way for the incoming deltas
(modulo bugs and inefficiencies in the ZSet implementations).

I am currently using two different paths for standard queries and incremental queries.
In Theory™ this is not necessary as you can model standard queries also as zsets (the delta is your database state),
but this will likely have significant overhead and is making initial exploratory programming a lot harder.

## Other projects in this space

### DBSP
- [zsxf](https://github.com/saberstack/zsxf) - WIP Datomic API for DBSP, uses transducers for everything
- [wizard](https://github.com/jumraiya/wizard) - IVM for Datascript - hooks into datascript

### Datalog engines
- [datascript](https://github.com/tonsky/datascript) - Immutable database and Datalog query engine for Clojure, ClojureScript and JS
- [xtdb v1](https://github.com/xtdb/xtdb) - General-purpose bitemporal database for Datalog & graph queries.
- [asami](https://github.com/quoll/asami) - Graph database written Clojure(script)
