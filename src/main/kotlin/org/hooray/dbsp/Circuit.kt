package org.hooray.dbsp

/**
 * A synchronous dataflow circuit: an immutable DAG of [Operator]s.
 *
 * Built once via `addInput` / `addUnary` / `addBinary`, then
 * driven one clock cycle at a time with [step]. Each [step] evaluates every
 * operator exactly once.
 *
 * **Scheduling.** The builder API cannot express a cycle: wiring an operator
 * requires [Stream]s that already exist, so a node's predecessors always have
 * lower ids than the node itself. Insertion order is therefore a valid
 * topological order, and [step] simply evaluates nodes in that order — each
 * operator sees the current-step output of its predecessors.
 *
 * The circuit freezes on the first [step]; no operators can be added after.
 */
class Circuit {

    private class Node(val name: String, val inputs: IntArray, val compute: () -> Any?) {
        var output: Any? = null
    }

    private val nodes = mutableListOf<Node>()
    private val sinks = mutableListOf<() -> Unit>()
    private var inputCount = 0
    private var frozen = false

    /** Number of operator nodes in the circuit. */
    val nodeCount: Int get() = nodes.size

    /** Operator names in evaluation order — useful for inspecting a built circuit. */
    fun operatorNames(): List<String> = nodes.map { it.name }

    /**
     * Per-node input ids in evaluation order — together with [operatorNames]
     * this is the circuit's full wiring.
     */
    fun nodeInputs(): List<List<Int>> = nodes.map { it.inputs.toList() }

    private fun checkBuildable() {
        check(!frozen) { "circuit is frozen: operators cannot be added after the first step" }
    }

    /**
     * Adds an input source, named `input-<n>` by creation order. Returns the
     * [Stream] to wire downstream and the [InputHandle] used to `push` a
     * value before each [step].
     */
    fun <D> addInput(): Pair<Stream<D>, InputHandle<D>> {
        checkBuildable()
        val handle = InputHandle<D>()
        val node = Node("input-$inputCount", IntArray(0)) {
            handle.poll() ?: error("circuit input was not pushed before step")
        }
        inputCount++
        val id = nodes.size
        nodes.add(node)
        return Stream<D>(id) to handle
    }

    /** Adds a unary operator wired to [input]. */
    fun <I, O> addUnary(operator: UnaryOperator<I, O>, input: Stream<I>): Stream<O> {
        checkBuildable()
        val inputNode = nodes[input.nodeId]
        val id = nodes.size
        nodes.add(Node(operator.name, intArrayOf(input.nodeId)) {
            @Suppress("UNCHECKED_CAST")
            operator.eval(inputNode.output as I)
        })
        return Stream(id)
    }

    /** Adds a binary operator wired to [left] and [right]. */
    fun <I1, I2, O> addBinary(
        operator: BinaryOperator<I1, I2, O>,
        left: Stream<I1>,
        right: Stream<I2>,
    ): Stream<O> {
        checkBuildable()
        val leftNode = nodes[left.nodeId]
        val rightNode = nodes[right.nodeId]
        val id = nodes.size
        nodes.add(Node(operator.name, intArrayOf(left.nodeId, right.nodeId)) {
            @Suppress("UNCHECKED_CAST")
            operator.eval(leftNode.output as I1, rightNode.output as I2)
        })
        return Stream(id)
    }

    /** Exposes [stream] as a circuit output, read via [OutputHandle.get] after each [step]. */
    fun <D> output(stream: Stream<D>): OutputHandle<D> {
        checkBuildable()
        val handle = OutputHandle<D>()
        val streamNode = nodes[stream.nodeId]
        sinks.add {
            @Suppress("UNCHECKED_CAST")
            handle.set(streamNode.output as D)
        }
        return handle
    }

    /** Runs one clock cycle: evaluates every operator once, then publishes outputs. */
    fun step() {
        frozen = true
        for (node in nodes) {
            node.output = node.compute()
        }
        for (sink in sinks) {
            sink()
        }
    }
}
