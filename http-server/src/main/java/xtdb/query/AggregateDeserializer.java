package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;

import java.io.IOException;

public class AggregateDeserializer extends StdDeserializer<Query.Aggregate> {

    public AggregateDeserializer() {
        super(Query.Aggregate.class);
    }

    public Query.Aggregate deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec mapper = p.getCodec();
        ObjectNode node = mapper.readTree(p);

        if (!node.isObject() || !node.has("aggregate")) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-aggregate"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }
        return Query.aggregate(SpecListDeserializer.<ColSpec>nodeToSpecs(mapper, node.get("aggregate"), ColSpec::of));
    }
}
