package org.hooray.util

import clojure.lang.Associative
import clojure.lang.Counted
import clojure.lang.IFn
import clojure.lang.ILookup
import clojure.lang.IMeta
import clojure.lang.IObj
import clojure.lang.IPersistentCollection
import clojure.lang.IPersistentMap
import clojure.lang.MapEquivalence
import clojure.lang.Reversible
import clojure.lang.Seqable

interface IPersistentSortedMap:
    IMeta, IObj, Counted, IPersistentCollection, IFn, Seqable, Reversible, ILookup, Associative, MapEquivalence, IPersistentMap, Map<Any,Any>
