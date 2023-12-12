package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class OutSpecDeserializer extends StdDeserializer<OutSpec> {

    public OutSpecDeserializer() {
        super(OutSpec.class);
    }

    @Override
    public OutSpec deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        TypeFactory typeFactory = mapper.getTypeFactory();
        JsonNode node = mapper.readTree(p);

        try {
            if (node.isTextual()) {
                String var = node.asText();
                return OutSpec.of(var, Expr.lVar(var));
            } else if (node.isObject()) {
                ObjectNode objectNode = (ObjectNode) node;
                Iterator<Map.Entry<String, JsonNode>> itr = objectNode.fields();
                Map.Entry<String, JsonNode> entry = itr.next();
                return OutSpec.of(entry.getKey(), mapper.treeToValue(entry.getValue(), Expr.class));
            } else {
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-out-spec"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-out-spec"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
    }
}
