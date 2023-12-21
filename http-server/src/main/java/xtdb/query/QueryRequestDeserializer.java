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

import java.io.IOException;

public class QueryRequestDeserializer extends StdDeserializer<QueryRequest> {

    public QueryRequestDeserializer() {
        super(QueryRequest.class);
    }

    public QueryRequest deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if(!node.isObject()) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-query-map"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        Query query;
        if (node.has("query")) {
            query = codec.treeToValue(node.get("query"), Query.class);
        } else {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "missing-query"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        QueryOpts queryOpts = null;
        if (node.has("query_opts")) {
            queryOpts = codec.treeToValue(node.get("query_opts"), QueryOpts.class);
        }

        return new QueryRequest(query, queryOpts);
    }
}