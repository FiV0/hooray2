package org.hooray.engine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class BindingSetTest {

    @Test
    fun `stores variable layout and correlated rows`() {
        val bindings = BindingSet(
            variables = listOf("?e", "?age"),
            rows = listOf(
                listOf("a", 35),
                listOf("b", 40),
            ),
        )

        assertEquals(listOf("?e", "?age"), bindings.variables)
        assertEquals(2, bindings.rowCount)
        assertEquals("a", bindings.valueAt(0, "?e"))
        assertEquals(35, bindings.valueAt(0, "?age"))
        assertEquals(40, bindings.valueAt(1, "?age"))
    }

    @Test
    fun `rejects rows whose arity does not match the variable layout`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            BindingSet(
                variables = listOf("?e", "?age"),
                rows = listOf(listOf("a")),
            )
        }

        assertEquals("Row 0 has arity 1, expected 2", error.message)
    }

    @Test
    fun `rejects duplicate variables`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            BindingSet(
                variables = listOf("?e", "?e"),
                rows = listOf(listOf("a", "b")),
            )
        }

        assertEquals("BindingSet variables must be distinct", error.message)
    }

    @Test
    fun `extends rows with introduced variables`() {
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(
                listOf("a"),
                listOf("b"),
            ),
        )

        val extended = input.extend(
            introducedVariables = listOf("?age", "?name"),
            extensions = listOf(
                RowExtension(inputRowIndex = 0, values = listOf(35, "A")),
                RowExtension(inputRowIndex = 1, values = listOf(40, "B")),
                RowExtension(inputRowIndex = 1, values = listOf(41, "Bee")),
            ),
        )

        assertEquals(listOf("?e", "?age", "?name"), extended.variables)
        assertEquals(
            listOf(
                listOf("a", 35, "A"),
                listOf("b", 40, "B"),
                listOf("b", 41, "Bee"),
            ),
            extended.rows,
        )
    }

    @Test
    fun `rejects row extensions with the wrong arity`() {
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a")),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            input.extend(
                introducedVariables = listOf("?age", "?name"),
                extensions = listOf(RowExtension(inputRowIndex = 0, values = listOf(35))),
            )
        }

        assertEquals("Extension for input row 0 has arity 1, expected 2", error.message)
    }

    @Test
    fun `deduplicates complete rows`() {
        val bindings = BindingSet(
            variables = listOf("?e", "?age"),
            rows = listOf(
                listOf("a", 35),
                listOf("a", 35),
                listOf("a", 36),
            ),
        )

        assertEquals(
            listOf(
                listOf("a", 35),
                listOf("a", 36),
            ),
            bindings.distinctRows().rows,
        )
    }

    @Test
    fun `reorders rows to a target layout`() {
        val bindings = BindingSet(
            variables = listOf("?e", "?age", "?name"),
            rows = listOf(
                listOf("a", 35, "A"),
                listOf("b", 40, "B"),
            ),
        )

        val reordered = bindings.reorder(listOf("?name", "?e", "?age"))

        assertEquals(listOf("?name", "?e", "?age"), reordered.variables)
        assertEquals(
            listOf(
                listOf("A", "a", 35),
                listOf("B", "b", 40),
            ),
            reordered.rows,
        )
    }

    @Test
    fun `reorder requires the same variables`() {
        val bindings = BindingSet(
            variables = listOf("?e", "?age"),
            rows = listOf(listOf("a", 35)),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            bindings.reorder(listOf("?e"))
        }

        assertEquals("Target layout must contain the same variables", error.message)
    }
}
