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
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.ArrayList;

public class PipelineDeserializer extends StdDeserializer<Query.Pipeline> {

    public PipelineDeserializer() {
        super(Query.Pipeline.class);
    }

    @Override
    public Query.Pipeline deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        ArrayNode node = mapper.readTree(p);

        try {
            Query query = mapper.treeToValue(node.get(0), Query.class);
            ArrayList<Query.QueryTail> tails = new ArrayList<>();
            var itr = node.iterator();
            itr.next();
            while(itr.hasNext()) {
                tails.add(mapper.treeToValue(itr.next(), Query.QueryTail.class));
            }
            return Query.pipeline(query, tails);
        } catch(Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-pipeline"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }


}
