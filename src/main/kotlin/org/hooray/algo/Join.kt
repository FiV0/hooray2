package org.hooray.algo

// TODO maybe make this an immutable list at some point
typealias ResultTuple = List<Any>

interface Join<T> {
    fun join(): List<T>
}