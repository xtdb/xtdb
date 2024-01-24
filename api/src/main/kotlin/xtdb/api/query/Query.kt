package xtdb.api.query

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*
import xtdb.api.query.Expr.Param
import xtdb.api.query.Exprs.lVar
import xtdb.api.query.XtqlQuery.OrderDirection.ASC
import xtdb.api.query.XtqlQuery.OrderDirection.DESC
import xtdb.api.query.XtqlQuery.OrderNulls.FIRST
import xtdb.api.query.XtqlQuery.OrderNulls.LAST
import xtdb.jsonIAE

@Serializable(Query.Serde::class)
sealed interface Query {
    /**
     * @suppress
     */
    object Serde : JsonContentPolymorphicSerializer<Query>(Query::class) {
        override fun selectDeserializer(element: JsonElement): DeserializationStrategy<Query> = when (element) {
            is JsonObject -> when {
                "sql" in element -> SqlQuery.serializer()
                else -> XtqlQuery.serializer()
            }

            else -> XtqlQuery.serializer()
        }
    }
}

@Serializable
data class SqlQuery(@JvmField val sql: String) : Query

@Serializable(XtqlQuery.Serde::class)
sealed interface XtqlQuery : Query {

    /**
     * @suppress
     */
    object Serde : JsonContentPolymorphicSerializer<XtqlQuery>(XtqlQuery::class) {
        override fun selectDeserializer(element: JsonElement): DeserializationStrategy<XtqlQuery> = when (element) {
            is JsonArray -> Pipeline.serializer()
            is JsonObject -> when {
                "from" in element -> From.serializer()
                "rel" in element -> Relation.serializer()
                "unify" in element -> Unify.serializer()
                "unionAll" in element -> UnionAll.serializer()
                else -> throw jsonIAE("xtql/malformed-query", element)
            }

            else -> throw jsonIAE("xtql/malformed-query", element)
        }
    }

    @Serializable(QueryTail.Serde::class)
    sealed interface QueryTail {
        /**
         * @suppress
         */
        object Serde : JsonContentPolymorphicSerializer<QueryTail>(QueryTail::class) {
            override fun selectDeserializer(element: JsonElement): DeserializationStrategy<QueryTail> = when (element) {
                is JsonObject -> when {
                    "aggregate" in element -> Aggregate.serializer()
                    "limit" in element -> Limit.serializer()
                    "offset" in element -> Offset.serializer()
                    "orderBy" in element -> OrderBy.serializer()
                    "return" in element -> Return.serializer()
                    "unnest" in element -> Unnest.serializer()
                    "where" in element -> Where.serializer()
                    "with" in element -> With.serializer()
                    "without" in element -> Without.serializer()
                    else -> throw jsonIAE("xtql/malformed-query-tail", element)
                }

                else -> throw jsonIAE("xtql/malformed-query-tail", element)
            }
        }
    }

    @Serializable(UnifyClause.Serde::class)
    sealed interface UnifyClause {
        /**
         * @suppress
         */
        object Serde : JsonContentPolymorphicSerializer<UnifyClause>(UnifyClause::class) {
            override fun selectDeserializer(element: JsonElement): DeserializationStrategy<UnifyClause> =
                when (element) {
                    is JsonObject -> when {
                        "call" in element -> Call.serializer()
                        "from" in element -> From.serializer()
                        "join" in element -> Join.serializer()
                        "leftJoin" in element -> LeftJoin.serializer()
                        "rel" in element -> Relation.serializer()
                        "unnest" in element -> Unnest.serializer()
                        "where" in element -> Where.serializer()
                        "with" in element -> With.serializer()
                        else -> throw jsonIAE("xtql/malformed-unify-clause", element)
                    }

                    else -> throw jsonIAE("xtql/malformed-unify-clause", element)
                }
        }
    }

    @Serializable(Pipeline.Serde::class)
    data class Pipeline(@JvmField val query: XtqlQuery, @JvmField val tails: List<QueryTail>) : XtqlQuery {
        internal object Serde : KSerializer<Pipeline> {
            override val descriptor: SerialDescriptor =
                buildClassSerialDescriptor("xtdb.api.query.Query.Pipeline", JsonArray.serializer().descriptor)

            override fun serialize(encoder: Encoder, value: Pipeline) {
                require(encoder is JsonEncoder)
                encoder.encodeJsonElement(buildJsonArray {
                    add(encoder.json.encodeToJsonElement(value.query))
                    for (tail in value.tails) add(encoder.json.encodeToJsonElement(tail))
                })
            }

            override fun deserialize(decoder: Decoder): Pipeline {
                require(decoder is JsonDecoder)
                val element = decoder.decodeJsonElement()
                if (element !is JsonArray) throw jsonIAE("xtql/malformed-pipeline", element)
                val query = decoder.json.decodeFromJsonElement<XtqlQuery>(element[0])
                val tails: MutableList<QueryTail> = mutableListOf()
                for (tailElement in element.subList(1, element.size))
                    tails.add(decoder.json.decodeFromJsonElement(tailElement))
                return Pipeline(query, tails)
            }
        }
    }

    @Serializable
    data class Unify(@JvmField @SerialName("unify") val clauses: List<UnifyClause>) : XtqlQuery

    @Serializable
    data class From(
        @JvmField @SerialName("from") val table: String,
        @JvmField @SerialName("bind") val bindings: List<Binding>? = null,
        @JvmField val forValidTime: TemporalFilter? = null,
        @JvmField val forSystemTime: TemporalFilter? = null,
        @JvmField val projectAllCols: Boolean = false,
    ) : XtqlQuery, UnifyClause {

        constructor(table: String, bindings: List<Binding>) : this(table, bindings, null, null, false)

        class Builder(private val table: String) : Binding.ABuilder<Builder, From>() {
            private var forValidTime: TemporalFilter? = null
            private var forSystemTime: TemporalFilter? = null
            private var projectAllCols: Boolean = false

            fun forValidTime(validTime: TemporalFilter?) = this.apply { this.forValidTime = validTime }
            fun forSystemTime(systemTime: TemporalFilter?) = this.apply { this.forSystemTime = systemTime }

            @JvmOverloads
            fun projectAllCols(projectAllCols: Boolean = true) = this.apply { this.projectAllCols = projectAllCols }

            override fun build() = From(table, buildBindings(), forValidTime, forSystemTime, projectAllCols)
        }
    }

    @Serializable
    data class Where(@JvmField @SerialName("where") val preds: List<Expr>) : QueryTail, UnifyClause

    @Serializable
    data class With(@JvmField @SerialName("with") val bindings: List<Binding>) : QueryTail, UnifyClause {
        class Builder : Binding.ABuilder<Builder, With>() {
            override fun build() = With(buildBindings())
        }
    }

    @Serializable
    data class Without(@JvmField @SerialName("without") val cols: List<String>) : QueryTail

    @Serializable
    data class Return(@JvmField @SerialName("return") val cols: List<Binding>) : QueryTail {
        class Builder : Binding.ABuilder<Builder, Return>() {
            override fun build() = Return(buildBindings())
        }
    }

    @Serializable
    data class Call(
        @JvmField @SerialName("call") val ruleName: String,
        @JvmField val args: List<Expr>,
        @JvmField @SerialName("bind") val bindings: List<Binding>? = null,
    ) : UnifyClause {

        fun binding(bindings: List<Binding>) = copy(bindings = bindings)
    }

    interface IJoin : UnifyClause {
        fun binding(bindings: List<Binding>): IJoin
    }

    @Serializable
    data class Join(
        @JvmField @SerialName("join") val query: XtqlQuery,
        @JvmField val args: List<Binding>? = null,
        @JvmField @SerialName("bind") val bindings: List<Binding>? = null,
    ) : IJoin {
        override fun binding(bindings: List<Binding>) = copy(bindings = bindings)
    }

    @Serializable
    data class LeftJoin(
        @JvmField @SerialName("leftJoin") val query: XtqlQuery,
        @JvmField val args: List<Binding>? = null,
        @JvmField @SerialName("bind") val bindings: List<Binding>? = null,
    ) : IJoin {

        override fun binding(bindings: List<Binding>) = copy(bindings = bindings)
    }

    @Serializable
    data class Aggregate(@JvmField @SerialName("aggregate") val cols: List<Binding>) : QueryTail {
        class Builder : Binding.ABuilder<Builder, Aggregate>() {
            override fun build() = Aggregate(buildBindings())
        }
    }

    @Serializable
    enum class OrderDirection {
        @SerialName("asc")
        ASC,

        @SerialName("desc")
        DESC
    }

    @Serializable
    enum class OrderNulls {
        @SerialName("first")
        FIRST,

        @SerialName("last")
        LAST
    }

    @Serializable(OrderSpec.Serde::class)
    data class OrderSpec(
        @JvmField val expr: Expr,
        @JvmField val direction: OrderDirection? = null,
        @JvmField val nulls: OrderNulls? = null,
    ) {
        fun asc() = copy(direction = ASC)
        fun desc() = copy(direction = DESC)
        fun nullsFirst() = copy(nulls = FIRST)
        fun nullsLast() = copy(nulls = LAST)

        internal object Serde : KSerializer<OrderSpec> {
            override val descriptor: SerialDescriptor = buildClassSerialDescriptor("xtdb.api.query.Query.OrderSpec")
            override fun serialize(encoder: Encoder, value: OrderSpec) {
                require(encoder is JsonEncoder)
                if (value.nulls == null && value.direction == null && value.expr is Expr.LogicVar) encoder.encodeString(
                    value.expr.lv
                )
                else encoder.encodeJsonElement(
                    buildJsonObject {
                        put("val", encoder.json.encodeToJsonElement(value.expr))
                        if (value.direction != null) {
                            put(
                                "dir", when (value.direction) {
                                    DESC -> "desc"
                                    else -> "asc"
                                }
                            )
                        }
                        if (value.nulls != null) {
                            put(
                                "nulls", when (value.nulls) {
                                    FIRST -> "first"
                                    else -> "last"
                                }
                            )
                        }
                    })
            }

            override fun deserialize(decoder: Decoder): OrderSpec {
                require(decoder is JsonDecoder)
                when (val element = decoder.decodeJsonElement()) {
                    is JsonPrimitive -> when {
                        element.isString -> return OrderSpec(element.contentOrNull?.let { lVar(it) }
                            ?: throw jsonIAE("xtql/malformed-order-spec", element))

                        else -> throw jsonIAE("xtql/malformed-order-spec", element)
                    }

                    is JsonObject -> {
                        val expr = decoder.json.decodeFromJsonElement<Expr>(
                            element["val"] ?: throw jsonIAE("xtql/malformed-order-spec", element)
                        )
                        val direction = element["dir"]?.let { decoder.json.decodeFromJsonElement<OrderDirection>(it) }
                        val nulls = element["nulls"]?.let { decoder.json.decodeFromJsonElement<OrderNulls>(it) }
                        return OrderSpec(expr, direction, nulls)
                    }

                    else -> throw jsonIAE("xtql/malformed-order-spec", element)
                }
            }
        }
    }

    @Serializable
    data class OrderBy(@JvmField @SerialName("orderBy") val orderSpecs: List<OrderSpec?>) : QueryTail

    @Serializable
    data class UnionAll(@JvmField @SerialName("unionAll") val queries: List<XtqlQuery>) : XtqlQuery

    @Serializable
    data class Limit(@JvmField @SerialName("limit") val length: Long) : QueryTail

    @Serializable
    data class Offset(@JvmField @SerialName("offset") val length: Long) : QueryTail

    @Serializable(Relation.Serde::class)
    abstract class Relation : XtqlQuery, UnifyClause {
        internal object Serde : JsonContentPolymorphicSerializer<Relation>(Relation::class) {
            override fun selectDeserializer(element: JsonElement): DeserializationStrategy<Relation> = when (element) {
                is JsonObject -> when {
                    "rel" in element ->
                        if (element["rel"] is JsonArray) DocsRelation.serializer()
                        else ParamRelation.serializer()

                    else -> throw jsonIAE("xtql/malformed-relation", element)

                }

                else -> throw jsonIAE("xtql/malformed-relation", element)
            }
        }
    }

    @Serializable
    data class DocsRelation(
        @JvmField @SerialName("rel") val documents: List<Map<String, Expr>>,
        @JvmField @SerialName("bind") val bindings: List<Binding>,
    ) :
        Relation() {
        fun bindings(bindings: List<Binding>) = copy(bindings = bindings)
    }

    @Serializable
    data class ParamRelation(
        @JvmField @SerialName("rel") val param: Param,
        @JvmField @SerialName("bind") val bindings: List<Binding?>,
    ) : Relation()

    @Serializable
    data class Unnest(@JvmField @SerialName("unnest") val binding: Binding) : QueryTail, UnifyClause
}

object Queries {
    @JvmStatic
    fun pipeline(query: XtqlQuery, tails: List<XtqlQuery.QueryTail>) = XtqlQuery.Pipeline(query, tails)

    @JvmStatic
    fun pipeline(query: XtqlQuery, vararg tails: XtqlQuery.QueryTail) = pipeline(query, tails.toList())

    @JvmStatic
    fun unify(clauses: List<XtqlQuery.UnifyClause>) = XtqlQuery.Unify(clauses)

    @JvmStatic
    fun unify(vararg clauses: XtqlQuery.UnifyClause) = unify(clauses.toList())

    @JvmStatic
    fun from(table: String) = XtqlQuery.From.Builder(table)

    @JvmSynthetic
    fun from(table: String, b: XtqlQuery.From.Builder.() -> Unit) = from(table).also { it.b() }.build()

    @JvmStatic
    fun where(preds: List<Expr>) = XtqlQuery.Where(preds)

    @JvmStatic
    fun where(vararg preds: Expr) = where(preds.toList())

    @JvmStatic
    fun with(vars: List<Binding>) = XtqlQuery.With(vars)

    @JvmStatic
    fun with() = XtqlQuery.With.Builder()

    @JvmSynthetic
    fun with(b: XtqlQuery.With.Builder.() -> Unit) = with().also { it.b() }.build()

    @JvmStatic
    fun without(cols: List<String>) = XtqlQuery.Without(cols)

    @JvmStatic
    fun without(vararg cols: String) = without(cols.toList())

    @JvmStatic
    fun returning(cols: List<Binding>) = XtqlQuery.Return(cols)

    @JvmStatic
    fun returning() = XtqlQuery.Return.Builder()

    @JvmSynthetic
    fun returning(b: XtqlQuery.Return.Builder.() -> Unit) = returning().also { it.b() }.build()

    @JvmStatic
    fun call(ruleName: String, args: List<Expr>) = XtqlQuery.Call(ruleName, args)

    @JvmStatic
    fun call(ruleName: String, vararg args: Expr) = XtqlQuery.Call(ruleName, args.toList())

    @JvmStatic
    fun join(query: XtqlQuery, args: List<Binding>? = null) = XtqlQuery.Join(query, args)

    @JvmStatic
    fun leftJoin(query: XtqlQuery, args: List<Binding>? = null) = XtqlQuery.LeftJoin(query, args)

    @JvmStatic
    fun aggregate(cols: List<Binding>) = XtqlQuery.Aggregate(cols)

    @JvmStatic
    fun aggregate() = XtqlQuery.Aggregate.Builder()

    @JvmSynthetic
    fun aggregate(b: XtqlQuery.Aggregate.Builder.() -> Unit) = aggregate().also { it.b() }.build()

    @JvmStatic
    fun orderSpec(col: String) = orderSpec(lVar(col))

    @JvmStatic
    fun orderSpec(expr: Expr) = XtqlQuery.OrderSpec(expr)

    @JvmStatic
    fun orderSpec(expr: Expr, direction: XtqlQuery.OrderDirection?, nulls: XtqlQuery.OrderNulls?) =
        XtqlQuery.OrderSpec(expr, direction, nulls)

    @JvmStatic
    fun orderBy(orderSpecs: List<XtqlQuery.OrderSpec>) = XtqlQuery.OrderBy(orderSpecs)

    @JvmStatic
    fun orderBy(vararg orderSpecs: XtqlQuery.OrderSpec) = orderBy(orderSpecs.toList())

    @JvmStatic
    fun unionAll(queries: List<XtqlQuery>) = XtqlQuery.UnionAll(queries)

    @JvmStatic
    fun unionAll(vararg queries: XtqlQuery) = unionAll(queries.toList())

    @JvmStatic
    fun limit(length: Long) = XtqlQuery.Limit(length)

    @JvmStatic
    fun offset(length: Long) = XtqlQuery.Offset(length)

    @JvmStatic
    fun relation(documents: List<Map<String, Expr>>, bindings: List<Binding>) =
        XtqlQuery.DocsRelation(documents, bindings)

    @JvmStatic
    fun relation(documents: List<Map<String, Expr>>, vararg bindings: Binding) =
        relation(documents, bindings.toList())

    @JvmStatic
    fun relation(param: Param, bindings: List<Binding>) = XtqlQuery.ParamRelation(param, bindings)

    @JvmStatic
    fun relation(param: Param, vararg bindings: Binding) = relation(param, bindings.toList())

    @JvmStatic
    fun unnest(binding: Binding) = XtqlQuery.Unnest(binding)
}
