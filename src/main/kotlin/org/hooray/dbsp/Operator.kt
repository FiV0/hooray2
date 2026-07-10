package org.hooray.dbsp

/**
 * Base type for all circuit operators.
 *
 * An operator is evaluated exactly once per [Circuit.step]. Stateful operators
 * (integrate, delay, incremental join) update their own state during `eval`;
 * the circuit's topological schedule guarantees every operator sees the
 * current-step output of its predecessors, so no separate commit phase is
 * needed.
 */
interface Operator {
    /** Human-readable name, used in diagnostics and circuit inspection. */
    val name: String
}

/** An operator with one input stream and one output stream. */
interface UnaryOperator<I, O> : Operator {
    fun eval(input: I): O
}

/** An operator with two input streams and one output stream. */
interface BinaryOperator<I1, I2, O> : Operator {
    fun eval(left: I1, right: I2): O
}
