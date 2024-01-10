package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;
import xtdb.api.query.Binding;
import xtdb.api.query.Query;
import xtdb.api.query.TemporalFilter;

import java.io.IOException;

public class FromDeserializer extends StdDeserializer<Query.From> {
    public FromDeserializer() {
        super(Query.From.class);
    }

    @Override
    public Query.From deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if (!node.isObject() || !node.has("from") || !node.has("bind")){
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-from"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
        var query = Query.from(node.get("from").asText());
        query = query.binding(SpecListDeserializer.<Binding>nodeToSpecs(codec, node.get("bind"), Binding::new));
        if (node.has("for_valid_time")) {
            query = query.forValidTime(codec.treeToValue(node.get("for_valid_time"), TemporalFilter.class));
        }
        if (node.has("for_system_time")) {
            query = query.forSystemTime(codec.treeToValue(node.get("for_system_time"), TemporalFilter.class));
        }
        return query;
    }
}
