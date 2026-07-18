package org.hooray.iterator

import clojure.data.avl.AVLMap
import clojure.data.avl.AVLSet

sealed interface AVLIndex {
    data class AVLMapIndex(val map: AVLMap) : AVLIndex
    data class AVLSetIndex(val set: AVLSet) : AVLIndex
}
