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
import xtdb.tx.Delete;

import java.io.IOException;
import java.time.Instant;

public class DeleteDeserializer extends StdDeserializer<Delete> {

    public DeleteDeserializer() {
        super(Delete.class);
    }

    @Override
    public Delete deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        BaseJsonNode node = mapper.readTree(p);

        try {
            ObjectNode objectNode = (ObjectNode) node;
            Delete op = new Delete(Keyword.intern(objectNode.get("delete").asText()), mapper.convertValue(objectNode.get("id"), Object.class));
            if (node.has("valid_from")) {
                op = op.startingFrom((Instant) mapper.treeToValue(objectNode.get("valid_from"), Object.class));
            }
            if (node.has("valid_to")) {
                op = op.until((Instant) mapper.treeToValue(objectNode.get("valid_to"), Object.class));
            }
            return op;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-delete"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}
