package org.hooray.incremental

/**
 * Interface representing a weight type that can be used in Z-sets.
 * Weights form a commutative group with addition.
 */
interface Weight<T : Weight<T>> {
    /**
     * Add this weight to another weight.
     */
    fun add(other: T): T

    /**
     * Negate this weight.
     */
    fun negate(): T

    /**
     * Check if this weight is zero.
     */
    fun isZero(): Boolean

    /**
     * Multiply this weight by a scalar.
     */
    fun multiply(scalar: Int): T

    /**
     * Multiply this weight by another weight.
     */
    fun multiply(other: T): T
}

/**
 * Integer weight implementation with overflow checking.
 */
data class IntegerWeight(val value: Int) : Weight<IntegerWeight> {
    override fun add(other: IntegerWeight): IntegerWeight {
        return IntegerWeight(Math.addExact(value, other.value))
    }

    override fun negate(): IntegerWeight {
        return IntegerWeight(-value)
    }

    override fun isZero(): Boolean {
        return value == 0
    }

    override fun multiply(scalar: Int): IntegerWeight {
        return IntegerWeight(Math.multiplyExact(value, scalar))
    }

    override fun multiply(other: IntegerWeight): IntegerWeight {
        return IntegerWeight(Math.multiplyExact(value, other.value))
    }

    companion object {
        @JvmField
        val ZERO = IntegerWeight(0)
        @JvmField
        val ONE = IntegerWeight(1)
        @JvmField
        val MINUS_ONE = IntegerWeight(-1)
    }
}
