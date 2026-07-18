package org.hooray.algo

// TODO maybe make this an immutable list at some point
typealias ResultTuple = List<Any>
typealias Prefix = ResultTuple
typealias Extension = Any

fun applyExtensions(prefix: Prefix, extensions: List<Extension>): List<ResultTuple> =
    extensions.map { extension -> prefix + extension }

interface Join<T> {
    fun join(): List<T>
}
