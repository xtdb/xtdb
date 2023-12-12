package xtdb.query;


import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class ArgSpecDeserializer extends StdDeserializer<ArgSpec> {

    public ArgSpecDeserializer() {
        super(OutSpec.class);
    }

    @Override
    public ArgSpec deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        try {
            if (node.isTextual()) {
                String var = node.asText();
                return ArgSpec.of(var, Expr.lVar(var));
            } else if (node.isObject()) {
                ObjectNode objectNode= (ObjectNode) node;
                Iterator<Map.Entry<String, JsonNode>> itr = objectNode.fields();
                Map.Entry<String, JsonNode> entry = itr.next();
                return ArgSpec.of(entry.getKey(), mapper.treeToValue(entry.getValue(), Expr.class));
            } else {
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-arg-spec"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-arg-spec"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
    }
}