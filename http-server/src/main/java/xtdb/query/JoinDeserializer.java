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
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinDeserializer extends StdDeserializer<Query.Join> {

    public JoinDeserializer() {
        super(Query.Join.class);
    }

    private List<ArgSpec> deserializeArgs(ObjectMapper mapper, ArrayNode node) throws JsonProcessingException {
        List<ArgSpec> res = new ArrayList<>();
        for (JsonNode argSpecNode : node) {
            res.add(mapper.treeToValue(argSpecNode, ArgSpec.class));
        }
        return res;
    }

    private List<OutSpec> deserializeBind(ObjectMapper mapper, ArrayNode node) throws JsonProcessingException {
        List<OutSpec> res = new ArrayList<>();
        for (JsonNode argSpecNode : node) {
            res.add(mapper.treeToValue(argSpecNode, OutSpec.class));
        }
        return res;
    }

    public Query.Join deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        ObjectNode node = mapper.readTree(p);

        try {
            var join = Query.join(mapper.treeToValue(node.get("join"), Query.class), deserializeArgs(mapper, (ArrayNode) node.get("args")));
            if (node.has("bind")) {
                join = join.binding(deserializeBind(mapper, (ArrayNode) node.get("bind")));
            }
            return join;
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-join"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}