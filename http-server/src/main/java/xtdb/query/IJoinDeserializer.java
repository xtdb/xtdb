package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.List;

public class IJoinDeserializer extends StdDeserializer<Query.IJoin> {

    public IJoinDeserializer() {
        super(Query.Join.class);
    }

    private List<ArgSpec> deserializeArgs(ObjectMapper mapper, ArrayNode node) throws Exception {
        return SpecListDeserializer.<ArgSpec>nodeToSpecs(mapper, node, ArgSpec::of);
    }

    private List<OutSpec> deserializeBind(ObjectMapper mapper, ArrayNode node) throws Exception {
        return SpecListDeserializer.<OutSpec>nodeToSpecs(mapper, node, OutSpec::of);
    }

    public Query.IJoin deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        ObjectNode node = mapper.readTree(p);

        try {
            Query.IJoin join = null;
            if (node.has("join")) {
                join = Query.join(mapper.treeToValue(node.get("join"), Query.class), deserializeArgs(mapper, (ArrayNode) node.get("args")));
            } else {
                join = Query.leftJoin(mapper.treeToValue(node.get("left_join"), Query.class), deserializeArgs(mapper, (ArrayNode) node.get("args")));
            }
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
