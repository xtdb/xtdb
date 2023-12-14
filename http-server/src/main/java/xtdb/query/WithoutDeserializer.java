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

public class WithoutDeserializer extends StdDeserializer<Query.Without> {

    public WithoutDeserializer() {
        super(Query.Without.class);
    }

    @Override
    public Query.Without deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);
        try {
            JsonNode without = node.get("without");
        
            if (without.isArray()) {
                List<String> cols = new ArrayList<>();
        
                for (JsonNode colNode : without) {
                    if (!colNode.isTextual()) {
                        throw new IllegalArgumentException("All items in Without clause must be strings", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
                    }
                    cols.add(colNode.asText());
                }
                        
                return Query.without(cols);
            } else {
                throw new IllegalArgumentException("Without should be a list of strings", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
            }
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-without"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}