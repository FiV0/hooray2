package org.hooray.algo

typealias ResultTuple = List<Any>

interface Join<T> {
    fun join(): List<T>
}