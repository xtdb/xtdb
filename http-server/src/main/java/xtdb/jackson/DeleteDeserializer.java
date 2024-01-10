package xtdb.jackson;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import xtdb.IllegalArgumentException;
import xtdb.api.tx.Delete;
import xtdb.api.tx.TxOp;

import java.io.IOException;
import java.time.Instant;

public class DeleteDeserializer extends StdDeserializer<Delete> {

    public DeleteDeserializer() {
        super(Delete.class);
    }

    @Override
    public Delete deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectCodec codec = p.getCodec();
        BaseJsonNode node = codec.readTree(p);

        if(!node.isObject()) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-delete"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        Delete op = TxOp.delete(node.get("delete").asText(), codec.treeToValue(node.get("id"), Object.class));
        if (node.has("valid_from")) {
            var instant = codec.treeToValue(node.get("valid_from"), Object.class);
            if (!(instant instanceof Instant)) {
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-delete"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
            op = op.startingFrom((Instant) instant);
        }
        if (node.has("valid_to")) {
            var instant = codec.treeToValue(node.get("valid_to"), Object.class);
            if (!(instant instanceof Instant)) {
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-delete"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
            op = op.until((Instant) instant);
        }
        return op;
    }
}
