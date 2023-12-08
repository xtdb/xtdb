package xtdb.jackson;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;
import xtdb.tx.Put;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

public class PutDeserializer extends StdDeserializer<Put> {

    public PutDeserializer() {
        super(Put.class);
    }

    @Override
    public Put deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        BaseJsonNode node =  mapper.readTree(p);

        try {
            ObjectNode objectNode = (ObjectNode) node;
            Put op = new Put(Keyword.intern(objectNode.get("put").asText()), mapper.treeToValue(objectNode.get("doc"), Map.class));
            if (objectNode.has("valid-from")) {
                op = op.startingFrom((Instant) mapper.treeToValue(objectNode.get("valid-from"), Object.class));
            }
            if (objectNode.has("valid-to")) {
                op = op.until((Instant) mapper.treeToValue(objectNode.get("valid-to"), Object.class));
            }
            return op;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-put"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
    }
}
