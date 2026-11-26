package xtdb.expression

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.arrow.CmpOp
import xtdb.arrow.IndirectVector
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.VectorIndirection.Mask
import xtdb.arrow.VectorReader
import xtdb.operator.SelectionSpec
import java.time.LocalDate

/**
 * A filter whose leaves compare a column with a literal or a param, combined by `and` and `or`, with every `not` already pushed down to the leaves.
 *
 * It is two-valued: a row is selected iff the predicate is true for it, so a negated leaf selects the rows that compare false, not those that don't compare true.
 */
sealed interface ScalarPredicate {

    /**
     * Resolves the params against this query's [args].
     * Null where a param's type is one the vectors can't compare with.
     */
    fun bind(args: RelationReader): Bound?

    sealed interface Operand

    sealed interface Literal : Operand {
        data object Null : Literal

        sealed interface Value : Literal {
            fun compare(vec: Vector, op: CmpOp, sel: IntArray?, mask: Mask): Mask?
        }

        data class Int64(val value: Long) : Value {
            override fun compare(vec: Vector, op: CmpOp, sel: IntArray?, mask: Mask) = vec.compare(op, value, sel, mask)
        }

        data class Float64(val value: Double) : Value {
            override fun compare(vec: Vector, op: CmpOp, sel: IntArray?, mask: Mask) = vec.compare(op, value, sel, mask)
        }

        data class Date(val value: LocalDate) : Value {
            override fun compare(vec: Vector, op: CmpOp, sel: IntArray?, mask: Mask) = vec.compare(op, value, sel, mask)
        }
    }

    data class Param(val name: String) : Operand {
        fun resolve(args: RelationReader): Literal? {
            val vec = args.vectorForOrNull(name) ?: return null
            if (vec.isNull(0)) return Literal.Null

            return when (vec.arrowType) {
                is ArrowType.Int -> Literal.Int64((vec.getObject(0) as Number).toLong())
                is ArrowType.FloatingPoint -> Literal.Float64((vec.getObject(0) as Number).toDouble())
                ArrowType.Date(DateUnit.DAY) -> Literal.Date(vec.getObject(0) as LocalDate)
                else -> null
            }
        }
    }

    data class Compare(val col: String, val op: CmpOp, val operand: Operand, val negated: Boolean) : ScalarPredicate {
        override fun bind(args: RelationReader): Bound? {
            val literal = when (operand) {
                is Literal -> operand
                is Param -> operand.resolve(args) ?: return null
            }

            return when (literal) {
                Literal.Null -> Bound.Unknown(col)
                is Literal.Value -> Bound.Compare(col, op, literal, negated)
            }
        }
    }

    data class And(val children: List<ScalarPredicate>) : ScalarPredicate {
        override fun bind(args: RelationReader): Bound? = Bound.And(children.map { it.bind(args) ?: return null })
    }

    data class Or(val children: List<ScalarPredicate>) : ScalarPredicate {
        override fun bind(args: RelationReader): Bound? = Bound.Or(children.map { it.bind(args) ?: return null })
    }

    /**
     * A [ScalarPredicate] with its params resolved for one query.
     */
    sealed interface Bound {

        /**
         * The rows of [mask] for which this is true.
         * Null where a column of [rel] can't answer it, or is absent.
         */
        fun select(rel: RelationReader, mask: Mask): Mask?

        data class Compare(val col: String, val op: CmpOp, val value: Literal.Value, val negated: Boolean) : Bound {
            override fun select(rel: RelationReader, mask: Mask): Mask? =
                rel.vectorForOrNull(col)?.onVector(mask) { vec, sel, m ->
                    val cmp = value.compare(vec, op, sel, m) ?: return@onVector null

                    // not `op`'s complement: against NaN, `x < NaN` and `x >= NaN` are both false
                    if (negated) vec.notNull(sel, m)?.andNot(cmp) else cmp
                }
        }

        /** A comparison with NULL: unknown on every row, so neither it nor its negation selects any. */
        data class Unknown(val col: String) : Bound {
            override fun select(rel: RelationReader, mask: Mask): Mask? =
                rel.vectorForOrNull(col)?.onVector(mask) { vec, sel, m ->
                    vec.notNull(sel, m)?.let { Mask.ofWords(LongArray(m.wordCount), m.rowCount) }
                }
        }

        data class And(val children: List<Bound>) : Bound {
            override fun select(rel: RelationReader, mask: Mask): Mask? {
                var acc = mask
                for (child in children) {
                    if (acc.cardinality == 0) break
                    acc = child.select(rel, acc) ?: return null
                }
                return acc
            }
        }

        data class Or(val children: List<Bound>) : Bound {
            override fun select(rel: RelationReader, mask: Mask): Mask? =
                children.map { it.select(rel, mask) ?: return null }.reduce(Mask::or)
        }
    }
}

private inline fun VectorReader.onVector(mask: Mask, f: (Vector, IntArray?, Mask) -> Mask?): Mask? =
    when (this) {
        is Vector -> f(this, null, mask)

        is IndirectVector -> when (val inner = inner) {
            is Vector -> when (val ind = sel) {
                is Mask -> f(inner, null, ind.andThen(mask))?.let(ind::extract)
                else -> f(inner, ind.idxs, mask)
            }

            else -> null
        }

        else -> null
    }

/**
 * Selects with [predicate] wherever the vectors can answer it, and otherwise with [generated]:
 * for the whole query where a param is of a type they can't compare with, and per batch where a column is.
 */
class ScalarPredicateSelection(
    private val predicate: ScalarPredicate,
    private val generated: SelectionSpec.Factory
) : SelectionSpec.Factory {

    override fun open(al: BufferAllocator, schema: Map<String, Any>, args: RelationReader): SelectionSpec {
        val bound = predicate.bind(args) ?: return generated.open(al, schema, args)

        return object : SelectionSpec {
            private var fallback: SelectionSpec? = null

            override fun select(readRelation: RelationReader, mask: Mask): Mask =
                bound.select(readRelation, mask)
                    ?: (fallback ?: generated.open(al, schema, args).also { fallback = it }).select(readRelation, mask)

            override fun close() {
                fallback?.close()
            }
        }
    }
}
