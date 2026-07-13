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
    fun `rejects unknown variables in column lookup`() {
        val bindings = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a")),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            bindings.columnIndex("?missing")
        }

        assertEquals("Unknown variable ?missing", error.message)
    }

    @Test
    fun `rejects unknown variables in value lookup`() {
        val bindings = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a")),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            bindings.valueAt(0, "?missing")
        }

        assertEquals("Unknown variable ?missing", error.message)
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
    fun `rejects duplicate introduced variables`() {
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a")),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            input.extend(
                introducedVariables = listOf("?age", "?age"),
                extensions = listOf(RowExtension(inputRowIndex = 0, values = listOf(35, 36))),
            )
        }

        assertEquals("Introduced variables must be distinct", error.message)
    }

    @Test
    fun `rejects introduced variables that are already bound`() {
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a")),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            input.extend(
                introducedVariables = listOf("?e"),
                extensions = listOf(RowExtension(inputRowIndex = 0, values = listOf("b"))),
            )
        }

        assertEquals("Introduced variables must not already be bound", error.message)
    }

    @Test
    fun `rejects row extensions with out of bounds input rows`() {
        val input = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a")),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            input.extend(
                introducedVariables = listOf("?age"),
                extensions = listOf(RowExtension(inputRowIndex = 1, values = listOf(35))),
            )
        }

        assertEquals("Input row index 1 is out of bounds", error.message)
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

    @Test
    fun `reorder rejects duplicate target variables`() {
        val bindings = BindingSet(
            variables = listOf("?e", "?age"),
            rows = listOf(listOf("a", 35)),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            bindings.reorder(listOf("?e", "?e"))
        }

        assertEquals("Target layout variables must be distinct", error.message)
    }

    @Test
    fun `indexes row positions by projected keys`() {
        val bindings = BindingSet(
            variables = listOf("?e", "?age", "?name"),
            rows = listOf(
                listOf("a", 35, "A"),
                listOf("a", 35, "Alias"),
                listOf("b", 40, "B"),
            ),
        )

        val index = bindings.indexBy(listOf("?age", "?e"))

        assertEquals(listOf("?age", "?e"), index.variables)
        assertEquals(
            linkedMapOf(
                listOf(35, "a") to listOf(0, 1),
                listOf(40, "b") to listOf(2),
            ),
            index.rowIndexesByKey,
        )
        assertEquals(null, index.rowIndexesByKey[listOf(50, "missing")])
    }

    @Test
    fun `indexes every row under the empty key`() {
        val bindings = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a"), listOf("b")),
        )

        assertEquals(
            mapOf(emptyList<Any>() to listOf(0, 1)),
            bindings.indexBy(emptyList()).rowIndexesByKey,
        )
    }

    @Test
    fun `index rejects duplicate and unknown variables`() {
        val bindings = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a")),
        )

        val duplicateError = assertThrows(IllegalArgumentException::class.java) {
            bindings.indexBy(listOf("?e", "?e"))
        }
        val unknownError = assertThrows(IllegalArgumentException::class.java) {
            bindings.indexBy(listOf("?missing"))
        }

        assertEquals("Index variables must be distinct", duplicateError.message)
        assertEquals("Unknown variable ?missing", unknownError.message)
    }

    @Test
    fun `selects rows in requested order and preserves repeated indexes`() {
        val bindings = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a"), listOf("b"), listOf("c")),
        )

        val selected = bindings.selectRows(listOf(2, 0, 2))

        assertEquals(listOf("?e"), selected.variables)
        assertEquals(listOf(listOf("c"), listOf("a"), listOf("c")), selected.rows)
        assertEquals(emptyList<BindingRow>(), bindings.selectRows(emptyList()).rows)
    }

    @Test
    fun `select rows rejects out of bounds indexes`() {
        val bindings = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a")),
        )

        val error = assertThrows(IllegalArgumentException::class.java) {
            bindings.selectRows(listOf(1))
        }

        assertEquals("Row index 1 is out of bounds", error.message)
    }

    @Test
    fun `projects and reorders variables without deduplicating rows`() {
        val bindings = BindingSet(
            variables = listOf("?e", "?age", "?name"),
            rows = listOf(
                listOf("a", 35, "A"),
                listOf("a", 36, "A"),
            ),
        )

        val projected = bindings.project(listOf("?name", "?e"))

        assertEquals(listOf("?name", "?e"), projected.variables)
        assertEquals(listOf(listOf("A", "a"), listOf("A", "a")), projected.rows)
        assertEquals(listOf(emptyList<Any>(), emptyList()), bindings.project(emptyList()).rows)
    }

    @Test
    fun `project rejects duplicate and unknown variables`() {
        val bindings = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a")),
        )

        val duplicateError = assertThrows(IllegalArgumentException::class.java) {
            bindings.project(listOf("?e", "?e"))
        }
        val unknownError = assertThrows(IllegalArgumentException::class.java) {
            bindings.project(listOf("?missing"))
        }

        assertEquals("Projection variables must be distinct", duplicateError.message)
        assertEquals("Unknown variable ?missing", unknownError.message)
    }

    @Test
    fun `natural join matches shared variables and multiplies witnesses`() {
        val left = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a"), listOf("a"), listOf("b")),
        )
        val right = BindingSet(
            variables = listOf("?age", "?e", "?name"),
            rows = listOf(
                listOf(35, "a", "A"),
                listOf(36, "a", "Alias"),
                listOf(40, "b", "B"),
            ),
        )

        val joined = left.join(right)

        assertEquals(listOf("?e", "?age", "?name"), joined.variables)
        assertEquals(
            listOf(
                listOf("a", 35, "A"),
                listOf("a", 36, "Alias"),
                listOf("a", 35, "A"),
                listOf("a", 36, "Alias"),
                listOf("b", 40, "B"),
            ),
            joined.rows,
        )
    }

    @Test
    fun `natural join forms a cartesian product without shared variables`() {
        val left = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a"), listOf("b")),
        )
        val right = BindingSet(
            variables = listOf("?age"),
            rows = listOf(listOf(35), listOf(40)),
        )

        assertEquals(
            listOf(
                listOf("a", 35),
                listOf("a", 40),
                listOf("b", 35),
                listOf("b", 40),
            ),
            left.join(right).rows,
        )
    }

    @Test
    fun `natural join matches every shared variable`() {
        val left = BindingSet(
            variables = listOf("?e", "?age"),
            rows = listOf(listOf("a", 35), listOf("a", 36)),
        )
        val right = BindingSet(
            variables = listOf("?age", "?e", "?name"),
            rows = listOf(listOf(35, "a", "A"), listOf(99, "a", "Wrong")),
        )

        assertEquals(
            BindingSet(
                variables = listOf("?e", "?age", "?name"),
                rows = listOf(listOf("a", 35, "A")),
            ),
            left.join(right),
        )
    }

    @Test
    fun `zero column relations have conventional join behavior`() {
        val unit = BindingSet(emptyList(), listOf(emptyList()))
        val empty = BindingSet(emptyList(), emptyList())
        val bindings = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a"), listOf("b")),
        )

        assertEquals(bindings, unit.join(bindings))
        assertEquals(bindings, bindings.join(unit))
        assertEquals(emptyList<BindingRow>(), empty.join(bindings).rows)
        assertEquals(emptyList<BindingRow>(), bindings.join(empty).rows)
    }

    @Test
    fun `semijoin and antijoin use existential right side support`() {
        val left = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a"), listOf("a"), listOf("b"), listOf("c")),
        )
        val right = BindingSet(
            variables = listOf("?age", "?e"),
            rows = listOf(
                listOf(35, "a"),
                listOf(36, "a"),
                listOf(40, "c"),
            ),
        )

        assertEquals(
            listOf(listOf("a"), listOf("a"), listOf("c")),
            left.semijoin(right).rows,
        )
        assertEquals(listOf(listOf("b")), left.antijoin(right).rows)
    }

    @Test
    fun `semi and antijoin without shared variables depend on right emptiness`() {
        val left = BindingSet(
            variables = listOf("?e"),
            rows = listOf(listOf("a"), listOf("b")),
        )
        val nonEmptyRight = BindingSet(
            variables = listOf("?age"),
            rows = listOf(listOf(35)),
        )
        val emptyRight = BindingSet(
            variables = listOf("?age"),
            rows = emptyList(),
        )

        assertEquals(left, left.semijoin(nonEmptyRight))
        assertEquals(emptyList<BindingRow>(), left.antijoin(nonEmptyRight).rows)
        assertEquals(emptyList<BindingRow>(), left.semijoin(emptyRight).rows)
        assertEquals(left, left.antijoin(emptyRight))
    }

    @Test
    fun `distinct union normalizes layout and keeps first occurrences`() {
        val left = BindingSet(
            variables = listOf("?e", "?age"),
            rows = listOf(listOf("a", 35), listOf("a", 35)),
        )
        val right = BindingSet(
            variables = listOf("?age", "?e"),
            rows = listOf(listOf(35, "a"), listOf(40, "b"), listOf(40, "b")),
        )

        assertEquals(
            BindingSet(
                variables = listOf("?e", "?age"),
                rows = listOf(listOf("a", 35), listOf("b", 40)),
            ),
            left.unionDistinct(right),
        )
    }

    @Test
    fun `distinct union requires the same variables`() {
        val left = BindingSet(listOf("?e"), listOf(listOf("a")))
        val right = BindingSet(listOf("?age"), listOf(listOf(35)))

        val error = assertThrows(IllegalArgumentException::class.java) {
            left.unionDistinct(right)
        }

        assertEquals("Union requires the same variables", error.message)
    }
}
