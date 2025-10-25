/*
 * Copyright (c) Rich Hickey and contributors. All rights reserved.
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 * which can be found in the file epl-v10.html at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 * You must not remove this notice, or any other, from this software.
 *
 * Ported to Kotlin from Clojure's clojure.data.avl
 * Original author: Michał Marczyk
 */

package org.hooray

import clojure.lang.*
import java.io.Serializable
import java.util.Comparator
import java.util.concurrent.atomic.AtomicReference

// ============================================================================
// Custom Interfaces
// ============================================================================

//internal interface IAVLNode<K, V> {
//    fun getKey(): K
//    fun setKey(k: K): IAVLNode<K, V>
//    fun getVal(): V
//    fun setVal(v: V): IAVLNode<K, V>
//    fun getLeft(): IAVLNode<K, V>?
//    fun setLeft(l: IAVLNode<K, V>?): IAVLNode<K, V>
//    fun getRight(): IAVLNode<K, V>?
//    fun setRight(r: IAVLNode<K, V>?): IAVLNode<K, V>
//    fun getHeight(): Int
//    fun setHeight(h: Int): IAVLNode<K, V>
//    fun getRank(): Int
//    fun setRank(r: Int): IAVLNode<K, V>
//}
//
//internal interface IAVLTree<K, V> {
//    fun getTree(): IAVLNode<K, V>?
//}
//
//interface INavigableTree<K, V> {
//    fun nearest(test: Test, k: K): Map.Entry<K, V>?
//}
//
//enum class Test { LT, LTE, GT, GTE }
//
//// ============================================================================
//// AVLNode Implementation
//// ============================================================================
//
//internal class AVLNode<K, V>(
//    private val edit: AtomicReference<Thread?>?,
//    override var key: K,
//    override var value: V,
//    private var left: IAVLNode<K, V>?,
//    private var right: IAVLNode<K, V>?,
//    private var height: Int,
//    private var rank: Int
//) : IAVLNode<K, V>,
//    IHashEq,
//    Indexed,
//    Counted,
//    IMeta,
//    IObj,
//    IPersistentCollection,
//    IPersistentStack,
//    IPersistentVector,
//    Reversible,
//    Associative,
//    ILookup,
//    IFn,
//    Seqable,
//    Sequential,
//    IEditableCollection,
//    IMapEntry,
//    Serializable,
//    Comparable<IPersistentVector> {
//
//    // IAVLNode implementation
//    override fun setKey(k: K): IAVLNode<K, V> {
//        key = k
//        return this
//    }
//
//    override fun getVal(): V = value
//    override fun setVal(v: V): IAVLNode<K, V> {
//        value = v
//        return this
//    }
//
//    override fun getLeft(): IAVLNode<K, V>? = left
//    override fun setLeft(l: IAVLNode<K, V>?): IAVLNode<K, V> {
//        left = l
//        return this
//    }
//
//    override fun getRight(): IAVLNode<K, V>? = right
//    override fun setRight(r: IAVLNode<K, V>?): IAVLNode<K, V> {
//        right = r
//        return this
//    }
//
//    override fun getHeight(): Int = height
//    override fun setHeight(h: Int): IAVLNode<K, V> {
//        height = h
//        return this
//    }
//
//    override fun getRank(): Int = rank
//    override fun setRank(r: Int): IAVLNode<K, V> {
//        rank = r
//        return this
//    }
//
//    // Map.Entry implementation
//    override fun getKey(): K = key
//
//    // Indexed implementation
//    override fun nth(i: Int): Any? = when (i) {
//        0 -> key
//        1 -> value
//        else -> throw IndexOutOfBoundsException("nth index out of bounds in AVLNode")
//    }
//
//    override fun nth(i: Int, notFound: Any?): Any? = when (i) {
//        0 -> key
//        1 -> value
//        else -> notFound
//    }
//
//    // Counted implementation
//    override fun count(): Int = 2
//
//    // IPersistentVector implementation
//    override fun length(): Int = 2
//
//    override fun assocN(i: Int, `val`: Any?): IPersistentVector = when (i) {
//        0 -> PersistentVector.create(`val`, value)
//        1 -> PersistentVector.create(key, `val`)
//        else -> throw IndexOutOfBoundsException("assocN index out of bounds in AVLNode")
//    }
//
//    override fun cons(o: Any?): IPersistentVector =
//        PersistentVector.create(key, value, o)
//
//    override fun empty(): IPersistentCollection = PersistentVector.EMPTY
//
//    override fun equiv(o: Any?): Boolean = when (o) {
//        is IPersistentVector, is java.util.RandomAccess ->
//            o is Counted && o.count() == 2 && Util.equiv(key, RT.nth(o, 0)) && Util.equiv(value, RT.nth(o, 1))
//        is Sequential, is List<*> ->
//            o is Counted && o.count() == 2 && Util.equiv(key, RT.first(o)) && Util.equiv(value, RT.second(o))
//        else -> false
//    }
//
//    // IPersistentStack implementation
//    override fun peek(): Any? = value
//    override fun pop(): IPersistentStack = PersistentVector.create(key)
//
//    // Reversible implementation
//    override fun rseq(): ISeq = PersistentList.create(listOf(value, key)) as ISeq
//
//    // Associative implementation
//    override fun assoc(key: Any?, `val`: Any?): Associative {
//        return if (Util.isInteger(key)) {
//            assocN(key., `val`)
//        } else {
//            throw IllegalArgumentException("key must be integer")
//        }
//    }
//
//    override fun containsKey(key: Any?): Boolean {
//        return if (Util.isInteger(key)) {
//            val i = Util.toInt(key)
//            i == 0 || i == 1
//        } else {
//            false
//        }
//    }
//
//    override fun entryAt(key: Any?): IMapEntry? {
//        return if (Util.isInteger(key)) {
//            when (Util.toInt(key)) {
//                0 -> MapEntry(0, this.key)
//                1 -> MapEntry(1, value)
//                else -> null
//            }
//        } else {
//            null
//        }
//    }
//
//    // ILookup implementation
//    override fun valAt(key: Any?): Any? = valAt(key, null)
//
//    override fun valAt(key: Any?, notFound: Any?): Any? {
//        return if (Util.isInteger(key)) {
//            when (Util.toInt(key)) {
//                0 -> this.key
//                1 -> value
//                else -> notFound
//            }
//        } else {
//            notFound
//        }
//    }
//
//    // IFn implementation
//    override fun invoke(arg1: Any?): Any? {
//        return if (Util.isInteger(arg1)) {
//            when (Util.toInt(arg1)) {
//                0 -> key
//                1 -> value
//                else -> throw IndexOutOfBoundsException("invoke index out of bounds in AVLNode")
//            }
//        } else {
//            throw IllegalArgumentException("key must be integer")
//        }
//    }
//
//    override fun call(): Any = throw ArityException(0, "AVLNode")
//    override fun run(): Any = call()
//
//    override fun applyTo(arglist: ISeq?): Any {
//        return AFn.applyToHelper(this, arglist)
//    }
//
//    // Seqable implementation
//    override fun seq(): ISeq = PersistentList.create(listOf(key, value))
//
//    // IMapEntry implementation
//    override fun key(): Any? = key
//    override fun `val`(): Any? = value
//
//    // Comparable implementation
//    override fun compareTo(other: IPersistentVector): Int {
//        if (this === other) return 0
//        val vcnt = other.count()
//        return when {
//            2 < vcnt -> -1
//            2 > vcnt -> 1
//            else -> {
//                val comp = Util.compare(key, other.nth(0))
//                if (comp == 0) Util.compare(value, other.nth(1)) else comp
//            }
//        }
//    }
//
//    // Object methods
//    override fun equals(other: Any?): Boolean = when (other) {
//        this -> true
//        is IPersistentVector, is java.util.RandomAccess ->
//            other is Counted && other.count() == 2 &&
//            (key == RT.nth(other, 0)) && (value == RT.nth(other, 1))
//        is Sequential, is List<*> ->
//            other is Counted && other.count() == 2 &&
//            (key == RT.first(other)) && (value == RT.second(other))
//        else -> false
//    }
//
//    override fun hashCode(): Int {
//        var result = 31 + Util.hash(key)
//        result = 31 * result + Util.hash(value)
//        return result
//    }
//
//    override fun toString(): String = "[$key $value]"
//}
//
//// ============================================================================
//// Helper Functions
//// ============================================================================
//
//private fun <K, V> height(node: IAVLNode<K, V>?): Int = node?.getHeight() ?: 0
//
//private fun <K, V> ensureEditable(
//    edit: AtomicReference<Thread?>?,
//    node: AVLNode<K, V>?
//): AVLNode<K, V>? {
//    if (node == null) return null
//    val owner = edit?.get()
//    return when {
//        owner === Thread.currentThread() -> node
//        owner == null -> throw IllegalAccessError("Transient used after persistent! call")
//        else -> throw IllegalAccessError("Transient used by non-owner thread")
//    }
//}
//
//private fun <K, V> ensureEditable(edit: AtomicReference<Thread?>?): Boolean {
//    val owner = edit?.get()
//    return when {
//        owner === Thread.currentThread() -> true
//        owner == null -> throw IllegalAccessError("Transient used after persistent! call")
//        else -> throw IllegalAccessError("Transient used by non-owner thread")
//    }
//}
//
//// ============================================================================
//// Rotation Operations
//// ============================================================================
//
//private fun <K, V> rotateLeft(node: IAVLNode<K, V>): IAVLNode<K, V> {
//    val l = node.getLeft()
//    val r = node.getRight()!!
//    val rl = r.getLeft()
//    val rr = r.getRight()
//    val lh = height(l)
//    val rlh = height(rl)
//    val rrh = height(rr)
//    val rnk = node.getRank()
//    val rnkr = r.getRank()
//
//    return AVLNode(
//        null,
//        r.getKey(), r.getVal(),
//        AVLNode(
//            null,
//            node.getKey(), node.getVal(),
//            l, rl,
//            1 + maxOf(lh, rlh),
//            rnk
//        ),
//        rr,
//        maxOf(lh + 2, rlh + 2, rrh + 1),
//        rnk + rnkr + 1
//    )
//}
//
//private fun <K, V> rotateLeft(edit: AtomicReference<Thread?>, node: AVLNode<K, V>): IAVLNode<K, V> {
//    ensureEditable(edit, node)
//    val l = node.getLeft()
//    val r = node.getRight() as AVLNode<K, V>
//    ensureEditable(edit, r)
//    val rl = r.getLeft()
//    val rr = r.getRight()
//    val lh = height(l)
//    val rlh = height(rl)
//    val rrh = height(rr)
//    val rnk = node.getRank()
//    val rnkr = r.getRank()
//
//    r.setLeft(node)
//    r.setHeight(maxOf(lh + 2, rlh + 2, rrh + 1))
//    r.setRank(rnk + rnkr + 1)
//    node.setRight(rl)
//    node.setHeight(1 + maxOf(lh, rlh))
//    return r
//}
//
//private fun <K, V> rotateRight(node: IAVLNode<K, V>): IAVLNode<K, V> {
//    val r = node.getRight()
//    val l = node.getLeft()!!
//    val lr = l.getRight()
//    val ll = l.getLeft()
//    val rh = height(r)
//    val lrh = height(lr)
//    val llh = height(ll)
//    val rnk = node.getRank()
//    val rnkl = l.getRank()
//
//    return AVLNode(
//        null,
//        l.getKey(), l.getVal(),
//        ll,
//        AVLNode(
//            null,
//            node.getKey(), node.getVal(),
//            lr, r,
//            1 + maxOf(rh, lrh),
//            rnk - rnkl - 1
//        ),
//        maxOf(rh + 2, lrh + 2, llh + 1),
//        rnkl
//    )
//}
//
//private fun <K, V> rotateRight(edit: AtomicReference<Thread?>, node: AVLNode<K, V>): IAVLNode<K, V> {
//    ensureEditable(edit, node)
//    val r = node.getRight()
//    val l = node.getLeft() as AVLNode<K, V>
//    ensureEditable(edit, l)
//    val lr = l.getRight()
//    val ll = l.getLeft()
//    val rh = height(r)
//    val lrh = height(lr)
//    val llh = height(ll)
//    val rnk = node.getRank()
//    val rnkl = l.getRank()
//
//    l.setRight(node)
//    l.setHeight(maxOf(rh + 2, lrh + 2, llh + 1))
//    node.setLeft(lr)
//    node.setHeight(1 + maxOf(rh, lrh))
//    node.setRank(rnk - rnkl - 1)
//    return l
//}
//
//// ============================================================================
//// Lookup Operations
//// ============================================================================
//
//private fun <K, V> lookup(comp: Comparator<in K>, node: IAVLNode<K, V>?, k: K): IAVLNode<K, V>? {
//    if (node == null) return null
//
//    val c = comp.compare(k, node.getKey())
//    return when {
//        c == 0 -> node
//        c < 0 -> lookup(comp, node.getLeft(), k)
//        else -> lookup(comp, node.getRight(), k)
//    }
//}
//
//private fun <K, V> lookupNearest(
//    comp: Comparator<in K>,
//    node: IAVLNode<K, V>?,
//    test: Test,
//    k: K
//): IAVLNode<K, V>? {
//    val below = test == Test.LT || test == Test.LTE
//    val equal = test == Test.LTE || test == Test.GTE
//    val back: (Int) -> Boolean = if (below) { c -> c < 0 } else { c -> c > 0 }
//    val backward: (IAVLNode<K, V>) -> IAVLNode<K, V>? = if (below) { n -> n.getLeft() } else { n -> n.getRight() }
//    val forward: (IAVLNode<K, V>) -> IAVLNode<K, V>? = if (below) { n -> n.getRight() } else { n -> n.getLeft() }
//
//    var prev: IAVLNode<K, V>? = null
//    var current = node
//
//    while (current != null) {
//        val c = comp.compare(k, current.getKey())
//        when {
//            c == 0 -> return if (equal) current else {
//                lookupNearest(comp, backward(current), test, k) ?: prev
//            }
//            back(c) -> current = backward(current)
//            else -> {
//                prev = current
//                current = forward(current)
//            }
//        }
//    }
//    return prev
//}
//
//private fun <K, V> select(node: IAVLNode<K, V>?, rank: Int): IAVLNode<K, V>? {
//    if (node == null) return null
//
//    val nodeRank = node.getRank()
//    return when {
//        nodeRank == rank -> node
//        nodeRank < rank -> select(node.getRight(), rank - nodeRank - 1)
//        else -> select(node.getLeft(), rank)
//    }
//}
//
//private fun <K, V> rank(comp: Comparator<in K>, node: IAVLNode<K, V>?, k: K): Int {
//    if (node == null) return -1
//
//    val c = comp.compare(k, node.getKey())
//    return when {
//        c == 0 -> node.getRank()
//        c < 0 -> rank(comp, node.getLeft(), k)
//        else -> {
//            val r = rank(comp, node.getRight(), k)
//            if (r == -1) -1 else node.getRank() + r + 1
//        }
//    }
//}
//
//// ============================================================================
//// Rebalance Operations
//// ============================================================================
//
//private fun <K, V> maybeRebalance(node: IAVLNode<K, V>): IAVLNode<K, V> {
//    val l = node.getLeft()
//    val r = node.getRight()
//    val lh = height(l)
//    val rh = height(r)
//    val b = lh - rh
//
//    return when {
//        // Right-heavy
//        b < -1 -> {
//            val rl = r!!.getLeft()
//            val rr = r.getRight()
//            val rlh = height(rl)
//            val rrh = height(rr)
//
//            if (rlh - rrh == 1) {
//                // Left-heavy
//                val newRight = rotateRight(r)
//                rotateLeft(
//                    AVLNode(
//                        null,
//                        node.getKey(), node.getVal(),
//                        node.getLeft(), newRight,
//                        1 + maxOf(lh, height(newRight)),
//                        node.getRank()
//                    )
//                )
//            } else {
//                rotateLeft(node)
//            }
//        }
//        // Left-heavy
//        b > 1 -> {
//            val ll = l!!.getLeft()
//            val lr = l.getRight()
//            val llh = height(ll)
//            val lrh = height(lr)
//
//            if (lrh - llh == 1) {
//                // Right-heavy
//                val newLeft = rotateLeft(l)
//                rotateRight(
//                    AVLNode(
//                        null,
//                        node.getKey(), node.getVal(),
//                        newLeft, node.getRight(),
//                        1 + maxOf(rh, height(newLeft)),
//                        node.getRank()
//                    )
//                )
//            } else {
//                rotateRight(node)
//            }
//        }
//        else -> node
//    }
//}
//
//private fun <K, V> maybeRebalance(edit: AtomicReference<Thread?>, node: AVLNode<K, V>): IAVLNode<K, V> {
//    val l = node.getLeft()
//    val r = node.getRight()
//    val lh = height(l)
//    val rh = height(r)
//    val b = lh - rh
//
//    return when {
//        // Right-heavy
//        b < -1 -> {
//            ensureEditable(edit, node)
//            val rl = r!!.getLeft()
//            val rr = r.getRight()
//            val rlh = height(rl)
//            val rrh = height(rr)
//
//            if (rlh - rrh == 1) {
//                val newRight = rotateRight(edit, r as AVLNode<K, V>)
//                node.setRight(newRight)
//                node.setHeight(1 + maxOf(lh, height(newRight)))
//                rotateLeft(edit, node)
//            } else {
//                rotateLeft(edit, node)
//            }
//        }
//        // Left-heavy
//        b > 1 -> {
//            ensureEditable(edit, node)
//            val ll = l!!.getLeft()
//            val lr = l.getRight()
//            val llh = height(ll)
//            val lrh = height(lr)
//
//            if (lrh - llh == 1) {
//                val newLeft = rotateLeft(edit, l as AVLNode<K, V>)
//                node.setLeft(newLeft)
//                node.setHeight(1 + maxOf(rh, height(newLeft)))
//                rotateRight(edit, node)
//            } else {
//                rotateRight(edit, node)
//            }
//        }
//        else -> node
//    }
//}
//
//// ============================================================================
//// Insert Operations
//// ============================================================================
//
//private class Box(var value: Boolean)
//
//private fun <K, V> insert(
//    comp: Comparator<in K>,
//    node: IAVLNode<K, V>?,
//    k: K,
//    v: V,
//    found: Box
//): IAVLNode<K, V> {
//    if (node == null) {
//        return AVLNode(null, k, v, null, null, 1, 0)
//    }
//
//    val nk = node.getKey()
//    val c = comp.compare(k, nk)
//
//    return when {
//        c == 0 -> {
//            found.value = true
//            AVLNode(
//                null, k, v,
//                node.getLeft(), node.getRight(),
//                node.getHeight(), node.getRank()
//            )
//        }
//        c < 0 -> {
//            val newChild = insert(comp, node.getLeft(), k, v, found)
//            maybeRebalance(
//                AVLNode(
//                    null,
//                    nk, node.getVal(),
//                    newChild, node.getRight(),
//                    1 + maxOf(newChild.getHeight(), height(node.getRight())),
//                    if (found.value) node.getRank() else node.getRank() + 1
//                )
//            )
//        }
//        else -> {
//            val newChild = insert(comp, node.getRight(), k, v, found)
//            maybeRebalance(
//                AVLNode(
//                    null,
//                    nk, node.getVal(),
//                    node.getLeft(), newChild,
//                    1 + maxOf(height(node.getLeft()), newChild.getHeight()),
//                    node.getRank()
//                )
//            )
//        }
//    }
//}
//
//private fun <K, V> insert(
//    edit: AtomicReference<Thread?>,
//    comp: Comparator<in K>,
//    node: IAVLNode<K, V>?,
//    k: K,
//    v: V,
//    found: Box
//): IAVLNode<K, V> {
//    if (node == null) {
//        return AVLNode(edit, k, v, null, null, 1, 0)
//    }
//
//    val mutableNode = node as AVLNode<K, V>
//    ensureEditable(edit, mutableNode)
//    val nk = node.getKey()
//    val c = comp.compare(k, nk)
//
//    when {
//        c == 0 -> {
//            found.value = true
//            mutableNode.setKey(k)
//            mutableNode.setVal(v)
//            return mutableNode
//        }
//        c < 0 -> {
//            val newChild = insert(edit, comp, node.getLeft(), k, v, found)
//            mutableNode.setLeft(newChild)
//            mutableNode.setHeight(1 + maxOf(newChild.getHeight(), height(node.getRight())))
//            if (!found.value) {
//                mutableNode.setRank(mutableNode.getRank() + 1)
//            }
//            return maybeRebalance(edit, mutableNode)
//        }
//        else -> {
//            val newChild = insert(edit, comp, node.getRight(), k, v, found)
//            mutableNode.setRight(newChild)
//            mutableNode.setHeight(1 + maxOf(height(node.getLeft()), newChild.getHeight()))
//            return maybeRebalance(edit, mutableNode)
//        }
//    }
//}
//
//// ============================================================================
//// Delete Operations
//// ============================================================================
//
//private fun <K, V> getRightmost(node: IAVLNode<K, V>): IAVLNode<K, V> {
//    val r = node.getRight()
//    return if (r != null) getRightmost(r) else node
//}
//
//private fun <K, V> deleteRightmost(node: IAVLNode<K, V>): IAVLNode<K, V>? {
//    val r = node.getRight()
//    if (r == null) return node.getLeft()
//
//    val l = node.getLeft()
//    val newRight = deleteRightmost(r)
//    return maybeRebalance(
//        AVLNode(
//            null,
//            node.getKey(), node.getVal(),
//            l, newRight,
//            1 + maxOf(height(l), height(newRight)),
//            node.getRank()
//        )
//    )
//}
//
//private fun <K, V> deleteRightmost(
//    edit: AtomicReference<Thread?>,
//    node: IAVLNode<K, V>?
//): IAVLNode<K, V>? {
//    if (node == null) return null
//
//    val mutableNode = node as AVLNode<K, V>
//    ensureEditable(edit, mutableNode)
//    val r = node.getRight()
//
//    return when {
//        r == null -> node.getLeft()?.let { ensureEditable(edit, it as AVLNode<K, V>) }
//        r.getRight() == null -> {
//            mutableNode.setRight(r.getLeft())
//            mutableNode.setHeight(1 + maxOf(height(node.getLeft()), height(r.getLeft())))
//            maybeRebalance(edit, mutableNode)
//        }
//        else -> {
//            val newRight = deleteRightmost(edit, r)
//            mutableNode.setRight(newRight)
//            mutableNode.setHeight(1 + maxOf(height(node.getLeft()), height(newRight)))
//            maybeRebalance(edit, mutableNode)
//        }
//    }
//}
//
//private fun <K, V> delete(
//    comp: Comparator<in K>,
//    node: IAVLNode<K, V>?,
//    k: K,
//    found: Box
//): IAVLNode<K, V>? {
//    if (node == null) return null
//
//    val nk = node.getKey()
//    val c = comp.compare(k, nk)
//
//    return when {
//        c == 0 -> {
//            val l = node.getLeft()
//            val r = node.getRight()
//            found.value = true
//
//            when {
//                l != null && r != null -> {
//                    val p = getRightmost(l)
//                    val lPrime = deleteRightmost(l)
//                    maybeRebalance(
//                        AVLNode(
//                            null,
//                            p.getKey(), p.getVal(),
//                            lPrime, r,
//                            1 + maxOf(height(lPrime), height(r)),
//                            node.getRank() - 1
//                        )
//                    )
//                }
//                else -> l ?: r
//            }
//        }
//        c < 0 -> {
//            val newChild = delete(comp, node.getLeft(), k, found)
//            if (newChild === node.getLeft()) {
//                node
//            } else {
//                maybeRebalance(
//                    AVLNode(
//                        null,
//                        nk, node.getVal(),
//                        newChild, node.getRight(),
//                        1 + maxOf(height(newChild), height(node.getRight())),
//                        if (found.value) node.getRank() - 1 else node.getRank()
//                    )
//                )
//            }
//        }
//        else -> {
//            val newChild = delete(comp, node.getRight(), k, found)
//            if (newChild === node.getRight()) {
//                node
//            } else {
//                maybeRebalance(
//                    AVLNode(
//                        null,
//                        nk, node.getVal(),
//                        node.getLeft(), newChild,
//                        1 + maxOf(height(node.getLeft()), height(newChild)),
//                        node.getRank()
//                    )
//                )
//            }
//        }
//    }
//}
//
//private fun <K, V> delete(
//    edit: AtomicReference<Thread?>,
//    comp: Comparator<in K>,
//    node: IAVLNode<K, V>?,
//    k: K,
//    found: Box
//): IAVLNode<K, V>? {
//    if (node == null) return null
//
//    val nk = node.getKey()
//    val c = comp.compare(k, nk)
//
//    return when {
//        c == 0 -> {
//            val l = node.getLeft()
//            val r = node.getRight()
//            found.value = true
//
//            when {
//                l != null && r != null -> {
//                    val mutableNode = node as AVLNode<K, V>
//                    ensureEditable(edit, mutableNode)
//                    val p = getRightmost(l)
//                    val lPrime = deleteRightmost(edit, l)
//                    mutableNode.setKey(p.getKey())
//                    mutableNode.setVal(p.getVal())
//                    mutableNode.setLeft(lPrime)
//                    mutableNode.setHeight(1 + maxOf(height(lPrime), height(r)))
//                    mutableNode.setRank(mutableNode.getRank() - 1)
//                    maybeRebalance(edit, mutableNode)
//                }
//                l != null -> l
//                r != null -> r
//                else -> null
//            }
//        }
//        c < 0 -> {
//            val newChild = delete(edit, comp, node.getLeft(), k, found)
//            if (found.value) {
//                val mutableNode = node as AVLNode<K, V>
//                ensureEditable(edit, mutableNode)
//                mutableNode.setLeft(newChild)
//                mutableNode.setHeight(1 + maxOf(height(newChild), height(node.getRight())))
//                mutableNode.setRank(mutableNode.getRank() - 1)
//                maybeRebalance(edit, mutableNode)
//            } else {
//                node
//            }
//        }
//        else -> {
//            val newChild = delete(edit, comp, node.getRight(), k, found)
//            if (found.value) {
//                val mutableNode = node as AVLNode<K, V>
//                ensureEditable(edit, mutableNode)
//                mutableNode.setRight(newChild)
//                mutableNode.setHeight(1 + maxOf(height(node.getLeft()), height(newChild)))
//                maybeRebalance(edit, mutableNode)
//            } else {
//                node
//            }
//        }
//    }
//}
//
//// ============================================================================
//// Sequence Operations
//// ============================================================================
//
//private fun <K, V> seqPush(
//    node: IAVLNode<K, V>?,
//    stack: List<IAVLNode<K, V>>,
//    ascending: Boolean
//): List<IAVLNode<K, V>> {
//    var current = node
//    var result = stack
//
//    while (current != null) {
//        result = result + current
//        current = if (ascending) current.getLeft() else current.getRight()
//    }
//    return result
//}
//
//private class AVLMapSeq<K, V>(
//    private var stack: List<IAVLNode<K, V>>,
//    private val ascending: Boolean,
//    private val cnt: Int
//) : ISeq, Sequential, Counted, IPersistentCollection, Serializable {
//
//    private var _hash: Int = -1
//    private var _hasheq: Int = -1
//
//    override fun first(): Any? = stack.last()
//
//    override fun next(): ISeq? {
//        val node = stack.last()
//        val nextStack = seqPush(
//            if (ascending) node.getRight() else node.getLeft(),
//            stack.dropLast(1),
//            ascending
//        )
//        return if (nextStack.isEmpty()) null else AVLMapSeq(nextStack, ascending, cnt - 1)
//    }
//
//    override fun more(): ISeq = next() ?: PersistentList.EMPTY
//
//    override fun cons(o: Any?): IPersistentCollection =
//        clojure.lang.Cons(o, this)
//
//    override fun empty(): IPersistentCollection = PersistentList.EMPTY
//
//    override fun equiv(o: Any?): Boolean {
//        if (o is Sequential || o is List<*>) {
//            var seq1: ISeq? = seq()
//            var seq2 = RT.seq(o)
//            while (seq1 != null && seq2 != null) {
//                if (!Util.equiv(seq1.first(), seq2.first())) return false
//                seq1 = seq1.next()
//                seq2 = seq2.next()
//            }
//            return seq1 == null && seq2 == null
//        }
//        return false
//    }
//
//    override fun seq(): ISeq = this
//
//    override fun count(): Int = if (cnt < 0) {
//        var c = 1
//        var s = next()
//        while (s != null) {
//            c++
//            s = s.next()
//        }
//        c
//    } else cnt
//
//    override fun hashCode(): Int {
//        if (_hash == -1) {
//            var hash = 1
//            var s: ISeq? = this
//            while (s != null) {
//                hash = 31 * hash + Util.hash(s.first())
//                s = s.next()
//            }
//            _hash = hash
//        }
//        return _hash
//    }
//
//    override fun equals(other: Any?): Boolean = APersistentMap.mapEquals(this, other)
//}
//
//private fun <K, V> createSeq(
//    node: IAVLNode<K, V>?,
//    ascending: Boolean,
//    cnt: Int
//): ISeq? {
//    if (node == null || cnt == 0) return null
//    return AVLMapSeq(seqPush(node, emptyList(), ascending), ascending, cnt)
//}
//
//// ============================================================================
//// AVLMap - Persistent Sorted Map
//// ============================================================================
//
//class AVLMap<K, V> private constructor(
//    private val comp: Comparator<in K>,
//    private val tree: IAVLNode<K, V>?,
//    private val cnt: Int,
//    private val _meta: IPersistentMap?
//) : APersistentMap(),
//    IAVLTree<K, V>,
//    INavigableTree<K, V>,
//    Indexed,
//    Sorted,
//    Reversible,
//    IEditableCollection,
//    IObj,
//    Serializable {
//
//    private var _hash: Int = -1
//    private var _hasheq: Int = -1
//
//    // IAVLTree implementation
//    override fun getTree(): IAVLNode<K, V>? = tree
//
//    // INavigableTree implementation
//    override fun nearest(test: Test, k: K): Map.Entry<K, V>? {
//        val node = lookupNearest(comp, tree, test, k)
//        return node?.let { MapEntry.create(it.getKey(), it.getVal()) }
//    }
//
//    // IMeta/IObj implementation
//    override fun meta(): IPersistentMap? = _meta
//
//    override fun withMeta(meta: IPersistentMap?): IObj =
//        AVLMap(comp, tree, cnt, meta)
//
//    // Counted implementation
//    override fun count(): Int = cnt
//
//    // Indexed implementation
//    override fun nth(i: Int): Any? {
//        val n = select(tree, i) ?: throw IndexOutOfBoundsException("nth index out of bounds in AVL tree")
//        return MapEntry.create(n.getKey(), n.getVal())
//    }
//
//    override fun nth(i: Int, notFound: Any?): Any? {
//        val n = select(tree, i)
//        return if (n != null) MapEntry.create(n.getKey(), n.getVal()) else notFound
//    }
//
//    // IPersistentCollection implementation
//    override fun cons(o: Any?): IPersistentCollection {
//        return when (o) {
//            is Map.Entry<*, *> -> {
//                @Suppress("UNCHECKED_CAST")
//                assoc(o.key as K, o.value as V)
//            }
//            is IPersistentVector -> assoc(o.nth(0) as K, o.nth(1) as V)
//            else -> {
//                var ret: IPersistentMap = this
//                val s = RT.seq(o)
//                var seq = s
//                while (seq != null) {
//                    val entry = seq.first() as Map.Entry<*, *>
//                    @Suppress("UNCHECKED_CAST")
//                    ret = ret.assoc(entry.key as K, entry.value as V)
//                    seq = seq.next()
//                }
//                ret
//            }
//        }
//    }
//
//    override fun empty(): IPersistentCollection = AVLMap<K, V>(comp, null, 0, _meta)
//
//    // Seqable implementation
//    override fun seq(): ISeq? = if (cnt > 0) createSeq(tree, true, cnt) else null
//
//    // Reversible implementation
//    override fun rseq(): ISeq? = if (cnt > 0) createSeq(tree, false, cnt) else null
//
//    // ILookup implementation
//    override fun valAt(key: Any?): Any? = valAt(key, null)
//
//    override fun valAt(key: Any?, notFound: Any?): Any? {
//        @Suppress("UNCHECKED_CAST")
//        val n = lookup(comp, tree, key as K)
//        return if (n != null) n.getVal() else notFound
//    }
//
//    // Associative implementation
//    override fun assoc(key: Any?, `val`: Any?): IPersistentMap {
//        @Suppress("UNCHECKED_CAST")
//        val found = Box(false)
//        val newTree = insert(comp, tree, key as K, `val` as V, found)
//        return AVLMap(comp, newTree, if (found.value) cnt else cnt + 1, _meta)
//    }
//
//    override fun containsKey(key: Any?): Boolean {
//        @Suppress("UNCHECKED_CAST")
//        return lookup(comp, tree, key as K) != null
//    }
//
//    override fun entryAt(key: Any?): IMapEntry? {
//        @Suppress("UNCHECKED_CAST")
//        val node = lookup(comp, tree, key as K)
//        return if (node != null) MapEntry.create(node.getKey(), node.getVal()) else null
//    }
//
//    // IPersistentMap implementation
//    override fun without(key: Any?): IPersistentMap {
//        @Suppress("UNCHECKED_CAST")
//        val found = Box(false)
//        val newTree = delete(comp, tree, key as K, found)
//        return if (found.value) {
//            AVLMap(comp, newTree, cnt - 1, _meta)
//        } else {
//            this
//        }
//    }
//
//    override fun assocEx(key: Any?, `val`: Any?): IPersistentMap {
//        @Suppress("UNCHECKED_CAST")
//        val found = Box(false)
//        val newTree = insert(comp, tree, key as K, `val` as V, found)
//        if (found.value) {
//            throw RuntimeException("Key already present")
//        }
//        return AVLMap(comp, newTree, cnt + 1, _meta)
//    }
//
//    // Sorted implementation
//    override fun seq(ascending: Boolean): ISeq? {
//        return if (cnt > 0) createSeq(tree, ascending, cnt) else null
//    }
//
//    override fun seqFrom(key: Any?, ascending: Boolean): ISeq? {
//        if (cnt == 0) return null
//
//        @Suppress("UNCHECKED_CAST")
//        val startNode = if (ascending) {
//            lookupNearest(comp, tree, Test.GTE, key as K)
//        } else {
//            lookupNearest(comp, tree, Test.LTE, key as K)
//        }
//
//        return if (startNode != null) {
//            AVLMapSeq(seqPush(startNode, emptyList(), ascending), ascending, -1)
//        } else {
//            null
//        }
//    }
//
//    override fun entryKey(entry: Any?): Any? = (entry as Map.Entry<*, *>).key
//
//    override fun comparator(): Comparator<*> = comp
//
//    // IEditableCollection implementation
//    override fun asTransient(): ITransientCollection =
//        AVLTransientMap(AtomicReference(Thread.currentThread()), comp, tree, cnt)
//
//    // Additional utility methods
//    fun rankOf(key: K): Int = rank(comp, tree, key)
//
//    override fun hashCode(): Int {
//        if (_hash == -1) {
//            _hash = APersistentMap.mapHash(this)
//        }
//        return _hash
//    }
//
//    override fun equals(other: Any?): Boolean = APersistentMap.mapEquals(this, other)
//
//    companion object {
//        @Suppress("UNCHECKED_CAST")
//        fun <K, V> create(comparator: Comparator<in K> = RT.DEFAULT_COMPARATOR as Comparator<in K>): AVLMap<K, V> {
//            return AVLMap(comparator, null, 0, null)
//        }
//
//        @Suppress("UNCHECKED_CAST")
//        fun <K, V> create(
//            vararg entries: Pair<K, V>,
//            comparator: Comparator<in K> = RT.DEFAULT_COMPARATOR as Comparator<in K>
//        ): AVLMap<K, V> {
//            var map = create<K, V>(comparator)
//            for ((k, v) in entries) {
//                map = map.assoc(k, v) as AVLMap<K, V>
//            }
//            return map
//        }
//    }
//}
//
//// ============================================================================
//// AVLTransientMap - Transient version
//// ============================================================================
//
//private class AVLTransientMap<K, V>(
//    private val edit: AtomicReference<Thread?>,
//    private val comp: Comparator<in K>,
//    private var tree: IAVLNode<K, V>?,
//    private var cnt: Int
//) : AFn(), ITransientMap, Counted {
//
//    override fun count(): Int {
//        ensureEditable()
//        return cnt
//    }
//
//    override fun valAt(key: Any?): Any? = valAt(key, null)
//
//    override fun valAt(key: Any?, notFound: Any?): Any? {
//        ensureEditable()
//        @Suppress("UNCHECKED_CAST")
//        val n = lookup(comp, tree, key as K)
//        return if (n != null) n.getVal() else notFound
//    }
//
//    override fun invoke(arg1: Any?): Any? = valAt(arg1)
//
//    override fun invoke(arg1: Any?, arg2: Any?): Any? = valAt(arg1, arg2)
//
//    override fun conj(o: Any?): ITransientCollection {
//        ensureEditable()
//        return when (o) {
//            is Map.Entry<*, *> -> {
//                @Suppress("UNCHECKED_CAST")
//                assoc(o.key, o.value)
//            }
//            is IPersistentVector -> assoc(o.nth(0), o.nth(1))
//            else -> {
//                var ret: ITransientMap = this
//                val s = RT.seq(o)
//                var seq = s
//                while (seq != null) {
//                    val entry = seq.first() as Map.Entry<*, *>
//                    ret = ret.assoc(entry.key, entry.value)
//                    seq = seq.next()
//                }
//                ret
//            }
//        }
//    }
//
//    override fun persistent(): IPersistentCollection {
//        ensureEditable()
//        edit.set(null)
//        return AVLMap(comp, tree, cnt, null)
//    }
//
//    override fun assoc(key: Any?, `val`: Any?): ITransientMap {
//        ensureEditable()
//        @Suppress("UNCHECKED_CAST")
//        val found = Box(false)
//        val newTree = insert(edit, comp, tree, key as K, `val` as V, found)
//        tree = newTree
//        if (!found.value) {
//            cnt++
//        }
//        return this
//    }
//
//    override fun without(key: Any?): ITransientMap {
//        ensureEditable()
//        @Suppress("UNCHECKED_CAST")
//        val found = Box(false)
//        val newTree = delete(edit, comp, tree, key as K, found)
//        if (found.value) {
//            tree = newTree
//            cnt--
//        }
//        return this
//    }
//
//    private fun ensureEditable() {
//        ensureEditable(edit)
//    }
//}
