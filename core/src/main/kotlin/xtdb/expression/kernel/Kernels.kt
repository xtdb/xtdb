package xtdb.expression.kernel

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.*
import xtdb.arrow.VectorIndirection.Companion.Selection
import xtdb.arrow.VectorIndirection.Companion.Slice
import xtdb.operator.ProjectionSpec
import xtdb.operator.SelectionSpec
import java.time.LocalDate

/**
 * A column as its physical base vector plus how logical row `r` maps onto it:
 * `phys(r) = (idxs?.get(r) ?: r) + offset`.
 */
internal class Physical(val base: VectorReader, val idxs: IntArray?, val offset: Int) {
    fun phys(r: Int) = (idxs?.get(r) ?: r) + offset
}

internal fun unwrap(v: VectorReader): Physical? = when (v) {
    is IndirectVector -> unwrap(v.inner)?.let { inner ->
        when (val sel = v.sel) {
            is Selection -> {
                val idxs = sel.idxs
                Physical(inner.base, IntArray(idxs.size) { inner.phys(idxs[it]) }, 0)
            }

            is Slice ->
                if (inner.idxs == null) Physical(inner.base, null, inner.offset + sel.startIdx)
                else Physical(inner.base, IntArray(sel.len) { inner.phys(sel.startIdx + it) }, 0)

            else -> Physical(inner.base, IntArray(sel.valueCount()) { inner.phys(sel.getIndex(it)) }, 0)
        }
    }

    is VectorReader.RenamedVector -> unwrap(v.inner)
    is DoubleVector, is DateDayVector, is LongVector, is IntVector -> Physical(v, null, 0)
    else -> null
}

sealed interface Scalar {
    fun resolve(args: RelationReader): Any?

    data class Lit(val v: Any?) : Scalar {
        override fun resolve(args: RelationReader) = v
    }

    data class Param(val name: String) : Scalar {
        override fun resolve(args: RelationReader): Any? =
            args.vectorForOrNull(name)?.let { if (it.isNull(0)) null else it.getObject(0) }
    }
}

/**
 * `lo (<|<=) col (<|<=) hi`, either bound optional.
 * `selectWithin` returns indices into `rel` (a subset of `inSel` if given), or null if this column/param shape isn't covered.
 */
class RangePredicate(
    val col: String,
    val lo: Scalar?, val loIncl: Boolean,
    val hi: Scalar?, val hiIncl: Boolean,
) {
    private object NullBound

    private fun Scalar?.resolveOrNull(args: RelationReader): Any? = this?.resolve(args) ?: if (this == null) null else NullBound

    fun selectWithin(rel: RelationReader, inSel: IntArray?, args: RelationReader): IntArray? {
        val vec = rel.vectorForOrNull(col) ?: return null
        val p = unwrap(vec) ?: return null
        val loV = lo.resolveOrNull(args)
        val hiV = hi.resolveOrNull(args)
        if (loV === NullBound || hiV === NullBound) return IntArray(0)

        val n = inSel?.size ?: rel.rowCount

        return when (val base = p.base) {
            is DoubleVector -> {
                val loD = if (loV == null) Double.NEGATIVE_INFINITY else (loV as? Number ?: return null).toDouble()
                val hiD = if (hiV == null) Double.POSITIVE_INFINITY else (hiV as? Number ?: return null).toDouble()
                selectDouble(base, p, n, inSel, loD, loV == null || loIncl, hiD, hiV == null || hiIncl)
            }

            is DateDayVector -> {
                val loL = when (loV) {
                    null -> Long.MIN_VALUE
                    is LocalDate -> loV.toEpochDay().let { if (loIncl) it else it + 1 }
                    else -> return null
                }
                val hiL = when (hiV) {
                    null -> Long.MAX_VALUE
                    is LocalDate -> hiV.toEpochDay().let { if (hiIncl) it else it - 1 }
                    else -> return null
                }
                selectInt(base, p, n, inSel, loL, hiL)
            }

            else -> null
        }
    }

    private fun selectDouble(
        base: DoubleVector, p: Physical, n: Int, inSel: IntArray?,
        lo: Double, loIncl: Boolean, hi: Double, hiIncl: Boolean
    ): IntArray {
        val data = base.dataBuffer
        val validity = base.validityBuffer
        val idxs = p.idxs
        val off = p.offset
        val out = IntArray(n)
        var k = 0
        for (i in 0 until n) {
            val r = inSel?.get(i) ?: i
            val ph = (idxs?.get(r) ?: r) + off
            if (validity != null && !validity.getBoolean(ph)) continue
            val x = data.getDouble(ph)
            val okLo = if (loIncl) x >= lo else x > lo
            val okHi = if (hiIncl) x <= hi else x < hi
            if (okLo && okHi) out[k++] = r
        }
        return if (k == n) out else out.copyOf(k)
    }

    private fun selectInt(base: DateDayVector, p: Physical, n: Int, inSel: IntArray?, lo: Long, hi: Long): IntArray {
        val data = base.dataBuffer
        val validity = base.validityBuffer
        val idxs = p.idxs
        val off = p.offset
        val out = IntArray(n)
        var k = 0
        for (i in 0 until n) {
            val r = inSel?.get(i) ?: i
            val ph = (idxs?.get(r) ?: r) + off
            if (validity != null && !validity.getBoolean(ph)) continue
            val x = data.getInt(ph).toLong()
            if (x in lo..hi) out[k++] = r
        }
        return if (k == n) out else out.copyOf(k)
    }

    override fun toString() = "(RangePredicate $col $lo $loIncl $hi $hiIncl)"
}

internal val KERNEL_DEBUG = java.lang.Boolean.getBoolean("xtdb.kernels.debug")
internal fun kdebug(msg: String) { if (KERNEL_DEBUG) System.err.println("[kernel] $msg") }

class KernelSelectionSpec(private val pred: RangePredicate, private val fallback: SelectionSpec) : SelectionSpec {
    init { kdebug("sel matched $pred") }
    private var warned = false
    private fun fellBack(): IntArray? { if (!warned) { warned = true; kdebug("sel FALLBACK $pred") }; return null }
    override fun select(allocator: BufferAllocator, readRelation: RelationReader, schema: Map<String, Any>, params: RelationReader): IntArray =
        (pred.selectWithin(readRelation, null, params) ?: fellBack())
            ?: fallback.select(allocator, readRelation, schema, params)

    override fun selectWithin(
        allocator: BufferAllocator, readRelation: RelationReader, inSel: IntArray?,
        schema: Map<String, Any>, params: RelationReader
    ): IntArray =
        (pred.selectWithin(readRelation, inSel, params) ?: fellBack())
            ?: super.selectWithin(allocator, readRelation, inSel, schema, params)
}

/** Lanewise f64 arithmetic over `+ - *` of f64 columns and numeric literals. */
sealed interface DoubleExpr {
    data class Col(val name: String) : DoubleExpr
    data class Lit(val v: Double) : DoubleExpr
    data class Bin(val op: Char, val l: DoubleExpr, val r: DoubleExpr) : DoubleExpr

    fun cols(): Set<String> = when (this) {
        is Col -> setOf(name)
        is Lit -> emptySet()
        is Bin -> l.cols() + r.cols()
    }
}

private class Lanes(val arr: DoubleArray?, val scalar: Double)

class DoubleProjectionKernel(private val expr: DoubleExpr) {
    private val colNames = expr.cols().toList()

    /** null if any column isn't a plain f64 vector */
    fun project(al: BufferAllocator, toName: String, rel: RelationReader): VectorReader? {
        val n = rel.rowCount
        val cols = HashMap<String, Physical>()
        for (c in colNames) {
            val p = unwrap(rel.vectorForOrNull(c) ?: return null) ?: return null
            if (p.base !is DoubleVector) return null
            cols[c] = p
        }

        var nulls: BooleanArray? = null
        for (p in cols.values) {
            val v = (p.base as DoubleVector).validityBuffer ?: continue
            val ns = nulls ?: BooleanArray(n).also { nulls = it }
            for (r in 0 until n) if (!v.getBoolean(p.phys(r))) ns[r] = true
        }

        val res = eval(expr, n, cols)
        val out = DoubleVector(al, toName, nulls != null)
        try {
            val arr = res.arr
            val ns = nulls
            if (arr == null) {
                for (r in 0 until n) if (ns != null && ns[r]) out.writeNull() else out.writeDouble(res.scalar)
            } else if (ns == null) {
                out.writeDoubles(arr, n)
            } else {
                for (r in 0 until n) if (ns[r]) out.writeNull() else out.writeDouble(arr[r])
            }
        } catch (e: Throwable) {
            out.close(); throw e
        }
        return out
    }

    private fun gather(p: Physical, n: Int): DoubleArray {
        val data = (p.base as DoubleVector).dataBuffer
        val idxs = p.idxs
        val off = p.offset
        val out = DoubleArray(n)
        if (idxs == null) for (r in 0 until n) out[r] = data.getDouble(r + off)
        else for (r in 0 until n) out[r] = data.getDouble(idxs[r] + off)
        return out
    }

    private fun eval(e: DoubleExpr, n: Int, cols: Map<String, Physical>): Lanes = when (e) {
        is DoubleExpr.Col -> Lanes(gather(cols.getValue(e.name), n), 0.0)
        is DoubleExpr.Lit -> Lanes(null, e.v)
        is DoubleExpr.Bin -> {
            val l = eval(e.l, n, cols)
            val r = eval(e.r, n, cols)
            val la = l.arr
            val ra = r.arr
            when {
                la == null && ra == null -> Lanes(null, op(e.op, l.scalar, r.scalar))
                // reuse an input array as the output - each is freshly allocated per batch
                la != null && ra != null -> {
                    when (e.op) {
                        '+' -> for (i in 0 until n) la[i] = la[i] + ra[i]
                        '-' -> for (i in 0 until n) la[i] = la[i] - ra[i]
                        '*' -> for (i in 0 until n) la[i] = la[i] * ra[i]
                    }
                    Lanes(la, 0.0)
                }

                la != null -> {
                    val s = r.scalar
                    when (e.op) {
                        '+' -> for (i in 0 until n) la[i] = la[i] + s
                        '-' -> for (i in 0 until n) la[i] = la[i] - s
                        '*' -> for (i in 0 until n) la[i] = la[i] * s
                    }
                    Lanes(la, 0.0)
                }

                else -> {
                    val s = l.scalar
                    ra!!
                    when (e.op) {
                        '+' -> for (i in 0 until n) ra[i] = s + ra[i]
                        '-' -> for (i in 0 until n) ra[i] = s - ra[i]
                        '*' -> for (i in 0 until n) ra[i] = s * ra[i]
                    }
                    Lanes(ra, 0.0)
                }
            }
        }
    }

    private fun op(op: Char, a: Double, b: Double) = when (op) {
        '+' -> a + b
        '-' -> a - b
        '*' -> a * b
        else -> error("op $op")
    }
}

class KernelProjectionSpec(
    private val kernel: DoubleProjectionKernel,
    private val fallback: ProjectionSpec
) : ProjectionSpec by fallback {
    init { kdebug("proj matched ${fallback.toName}") }
    private var warned = false
    private fun fellBack(): VectorReader? { if (!warned) { warned = true; kdebug("proj FALLBACK ${fallback.toName}") }; return null }
    override fun project(allocator: BufferAllocator, inRel: RelationReader, schema: Map<String, Any>, args: RelationReader): VectorReader =
        kernel.project(allocator, toName, inRel) ?: fellBack() ?: fallback.project(allocator, inRel, schema, args)
}
