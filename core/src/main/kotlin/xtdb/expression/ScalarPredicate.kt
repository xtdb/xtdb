package xtdb.expression

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import xtdb.arrow.CmpOp
import xtdb.arrow.RelationReader
import xtdb.arrow.Vector
import xtdb.arrow.VectorIndirection.Mask
import xtdb.operator.SelectionSpec
import java.time.LocalDate

/**
 * A filter comparing a column with a literal or a param.
 */
sealed interface ScalarPredicate {

    /**
     * Resolves the params against this query's [args].
     * Null where a param is null, or of a type the vectors can't compare with.
     */
    fun bind(args: RelationReader): Bound?

    sealed interface Operand

    sealed interface Literal : Operand {
        fun compare(vec: Vector, op: CmpOp, mask: Mask): Mask?

        data class Int64(val value: Long) : Literal {
            override fun compare(vec: Vector, op: CmpOp, mask: Mask) = vec.compare(op, value, mask)
        }

        data class Float64(val value: Double) : Literal {
            override fun compare(vec: Vector, op: CmpOp, mask: Mask) = vec.compare(op, value, mask)
        }

        data class Date(val value: LocalDate) : Literal {
            override fun compare(vec: Vector, op: CmpOp, mask: Mask) = vec.compare(op, value, mask)
        }
    }

    data class Param(val name: String) : Operand {
        fun resolve(args: RelationReader): Literal? {
            val vec = args.vectorForOrNull(name) ?: return null
            if (vec.isNull(0)) return null

            return when (vec.arrowType) {
                is ArrowType.Int -> Literal.Int64((vec.getObject(0) as Number).toLong())
                is ArrowType.FloatingPoint -> Literal.Float64((vec.getObject(0) as Number).toDouble())
                ArrowType.Date(DateUnit.DAY) -> Literal.Date(vec.getObject(0) as LocalDate)
                else -> null
            }
        }
    }

    data class Compare(val col: String, val op: CmpOp, val operand: Operand) : ScalarPredicate {
        override fun bind(args: RelationReader): Bound? {
            val literal = when (operand) {
                is Literal -> operand
                is Param -> operand.resolve(args) ?: return null
            }

            return Bound.Compare(col, op, literal)
        }
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

        data class Compare(val col: String, val op: CmpOp, val value: Literal) : Bound {
            override fun select(rel: RelationReader, mask: Mask): Mask? =
                (rel.vectorForOrNull(col) as? Vector)?.let { value.compare(it, op, mask) }
        }
    }
}

/**
 * Selects with [predicate] wherever the vectors can answer it, and otherwise with [generated]:
 * for the whole query where a param is null or of a type they can't compare with, and per batch where a column is.
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
