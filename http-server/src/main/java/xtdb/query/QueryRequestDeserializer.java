package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;
import xtdb.api.TransactionKey;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Map;

public class QueryRequestDeserializer extends StdDeserializer<QueryRequest> {

    public QueryRequestDeserializer() {
        super(QueryRequest.class);
    }

    public QueryRequest deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        TypeFactory typeFactory = mapper.getTypeFactory();
        JsonNode node = mapper.readTree(p);

        try {
            if (node.isObject()) {
                Query query;
                if (node.has("query")) {
                    query = mapper.treeToValue(node.get("query"), Query.class);
                } else {
                    throw IllegalArgumentException.create(Keyword.intern("xtql", "missing-query"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
                }

                QueryOpts queryOpts = null;
                if (node.has("query_opts")) {
                   queryOpts = mapper.treeToValue(node.get("query_opts"), QueryOpts.class);
                }

                return new QueryRequest(query, queryOpts);
            } else  {
                throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-query-map"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
        } catch (IllegalArgumentException e) {
          throw e;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-query-map"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}