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
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class VarSpecDeserializer extends StdDeserializer<VarSpec> {

    public VarSpecDeserializer() {
        super(OutSpec.class);
    }

    @Override
    public VarSpec deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        TypeFactory typeFactory = mapper.getTypeFactory();
        JsonNode node = mapper.readTree(p);

        try {
            if (node.isTextual()) {
                String var = node.asText();
                return VarSpec.of(var, Expr.lVar(var));
            } else if (node.isObject()) {
                ObjectNode objectNode= (ObjectNode) node;
                Iterator<Map.Entry<String, JsonNode>> itr = objectNode.fields();
                Map.Entry<String, JsonNode> entry = itr.next();
                return VarSpec.of(entry.getKey(), Expr.lVar(entry.getValue().asText()));
            } else {
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-var-spec"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-var-spec"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
    }
}