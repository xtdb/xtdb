package xtdb.jackson

import clojure.lang.Keyword
import clojure.lang.PersistentHashMap
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.BaseJsonNode
import xtdb.IllegalArgumentException
import xtdb.api.tx.Call
import java.io.IOException

class CallDeserializer : StdDeserializer<Call>(Call::class.java) {
    @Throws(IllegalArgumentException::class, IOException::class)
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): Call {
        val codec = p.codec
        val node = codec.readTree<BaseJsonNode>(p)

        if (!node.isObject || !node["args"].isArray) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-call"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()))
        }

        return Call(codec.treeToValue(node["call"], Any::class.java), codec.treeToValue(node["args"], List::class.java))
    }
}
