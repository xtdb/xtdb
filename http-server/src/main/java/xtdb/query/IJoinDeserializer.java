package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;
import xtdb.api.query.Binding;
import xtdb.api.query.Query;

import java.io.IOException;

public class IJoinDeserializer extends StdDeserializer<Query.IJoin> {

    public IJoinDeserializer() {
        super(Query.Join.class);
    }

    public Query.IJoin deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectCodec codec = p.getCodec();
        ObjectNode node = codec.readTree(p);

        if (!node.isObject() || !(node.has("join") || node.has("left_join")) || !node.has("args")) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-join"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        Query.IJoin join;

        if (node.has("join")) {
            join = Query.join(codec.treeToValue(node.get("join"), Query.class), SpecListDeserializer.<Binding>nodeToSpecs(codec, node.get("args"), Binding::new));
        } else {
            join = Query.leftJoin(codec.treeToValue(node.get("left_join"), Query.class), SpecListDeserializer.<Binding>nodeToSpecs(codec, node.get("args"), Binding::new));
        }

        if (node.has("bind")) {
            join = join.binding(SpecListDeserializer.<Binding>nodeToSpecs(codec, node.get("bind"), Binding::new));
        }

        return join;
    }
}
