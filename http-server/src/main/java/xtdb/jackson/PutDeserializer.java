package xtdb.jackson;

import clojure.lang.Keyword;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.tx.Ops;
import xtdb.tx.Put;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

public class PutDeserializer extends StdDeserializer<Put> {

    public PutDeserializer() {
        super(Put.class);
    }

    @Override
    public Put deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        ObjectNode node = mapper.readTree(p);

        Put op = new Put(Keyword.intern(node.get("put").asText()), mapper.treeToValue(node.get("doc"), Map.class));
        if (node.has("valid-from")) {
            op = op.startingFrom((Instant) mapper.treeToValue(node.get("valid-from"), Object.class));
        }
        if (node.has("valid-to")) {
            op = op.until((Instant) mapper.treeToValue(node.get("valid-to"), Object.class));
        }
        return op;
    }
}
