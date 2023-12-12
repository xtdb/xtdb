package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReturnDeserializer extends StdDeserializer<Query.Return> {

    public ReturnDeserializer() {
        super(Query.Return.class);
    }

    @Override
    public Query.Return deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);
        try {
            JsonNode returnNode = node.get("return");

            if (returnNode.isArray()) {
                List<ColSpec> cols = new ArrayList<>();

                for (JsonNode colNode : returnNode) {
                    ColSpec colSpec = mapper.treeToValue(colNode, ColSpec.class);
                    cols.add(colSpec);
                }

                return Query.returning(cols);
            } else {
                throw new Exception("Return should be a list of values");
            }
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-return"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
        
    }
}