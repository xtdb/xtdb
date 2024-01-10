package xtdb.query;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import xtdb.api.query.Query;

import java.io.IOException;
import java.util.ArrayList;

public class PipelineDeserializer extends StdDeserializer<Query.Pipeline> {

    public PipelineDeserializer() {
        super(Query.Pipeline.class);
    }

    @Override
    public Query.Pipeline deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec codec = p.getCodec();
        ArrayNode node = codec.readTree(p);

        Query query = codec.treeToValue(node.get(0), Query.class);
        ArrayList<Query.QueryTail> tails = new ArrayList<>();
        var itr = node.iterator();
        itr.next();
        while(itr.hasNext()) {
            tails.add(codec.treeToValue(itr.next(), Query.QueryTail.class));
        }
        return Query.pipeline(query, tails);
    }
}
