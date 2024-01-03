package xtdb.query;


import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class ColSpecDeserializer extends StdDeserializer<ColSpec> {

    public ColSpecDeserializer() {
        super(Binding.class);
    }

    @Override
    public ColSpec deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if (node.isTextual()) {
            String var = node.asText();
            return ColSpec.of(var, Expr.lVar(var));
        } else if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> itr = node.fields();
            Map.Entry<String, JsonNode> entry = itr.next();
            return ColSpec.of(entry.getKey(), codec.treeToValue(entry.getValue(), Expr.class));
        } else {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-col-spec"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
    }
}
