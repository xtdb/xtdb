package xtdb.jackson

import clojure.lang.Keyword
import clojure.lang.PersistentHashMap
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.fasterxml.jackson.databind.node.BaseJsonNode
import xtdb.IllegalArgumentException
import xtdb.tx.Sql
import java.io.IOException


class SqlOpDeserializer : StdDeserializer<Sql>(Sql::class.java) {
    @Throws(IllegalArgumentException::class, IOException::class)
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): Sql{
        val mapper = p.codec as ObjectMapper
        val node = mapper.readTree<BaseJsonNode>(p)

        if (!node.isObject || !node["sql"].isTextual) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-sql-op"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()))
        }

        var sql = Sql(node["sql"].asText())
        if (node.has("arg_rows")) {
            if (!node["arg_rows"].isArray){
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-sql-op"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()))
            }
            try {
                sql.withArgs(mapper.treeToValue(node["arg_rows"], mapper.typeFactory.constructCollectionType(List::class.java, List::class.java)) as List<List<*>>).also { sql = it }
            } catch (e : MismatchedInputException) {
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-sql-op"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()))
            }
        }

        return sql;
    }
}