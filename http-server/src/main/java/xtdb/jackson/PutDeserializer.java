package xtdb.jackson;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import xtdb.IllegalArgumentException;
import xtdb.api.tx.Put;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static xtdb.api.tx.TxOp.put;

public class PutDeserializer extends StdDeserializer<Put> {

    public PutDeserializer() {
        super(Put.class);
    }

    @Override
    public Put deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectCodec codec = p.getCodec();
        BaseJsonNode node =  codec.readTree(p);

        if(!node.isObject() || !node.has("put") || !node.has("doc") || !node.get("put").isTextual() || !node.get("doc").isObject()) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-put"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        Put op = put(node.get("put").asText(), codec.treeToValue(node.get("doc"), Map.class));
        if (node.has("valid_from")) {
            var instant = codec.treeToValue(node.get("valid_from"), Object.class);
            if (!(instant instanceof Instant)) {
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-put"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
            op = op.startingFrom((Instant) instant);
        }
        if (node.has("valid_to")) {
            var instant = codec.treeToValue(node.get("valid_to"), Object.class);
            if (!(instant instanceof Instant)) {
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-put"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
            op = op.until((Instant) instant);
        }
        return op;
    }
}
