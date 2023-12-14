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

    public Query.IJoin deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        ObjectNode node = mapper.readTree(p);

        try {
            Query.IJoin join = null;
            if (node.has("join")) {
                join = Query.join(mapper.treeToValue(node.get("join"), Query.class), SpecListDeserializer.<ArgSpec>nodeToSpecs(mapper, node.get("args"), ArgSpec::of));
            } else {
                join = Query.leftJoin(mapper.treeToValue(node.get("left_join"), Query.class), SpecListDeserializer.<ArgSpec>nodeToSpecs(mapper, node.get("args"), ArgSpec::of));
            }
            if (node.has("bind")) {
                join = join.binding(SpecListDeserializer.<OutSpec>nodeToSpecs(mapper, node.get("bind"), OutSpec::of));
            }
            return join;
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-join"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}
