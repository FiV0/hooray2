package org.hooray.util

import clojure.lang.ISeq
import clojure.lang.Seqable
import clojure.lang.Sequential
import me.tonsky.persistent_sorted_set.ISeek

interface IPersistentSortedMapSeq : Seqable, ISeq, Sequential, ISeek