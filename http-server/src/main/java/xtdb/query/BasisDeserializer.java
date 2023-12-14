package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;
import xtdb.api.TransactionKey;

import java.io.IOException;
import java.time.Instant;

public class BasisDeserializer extends StdDeserializer<Basis> {

    public BasisDeserializer() {
        super(Basis.class);
    }

    public Basis deserialize (JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);

        try {
            if (node instanceof ObjectNode) {
                TransactionKey atTx;
                Instant currentTime = null;
                if (node.has("current_time")) {
                    currentTime = (Instant) mapper.treeToValue(node.get("current_time"), Object.class);
                }
                if (node.has("at_tx")) {
                    return new Basis(mapper.treeToValue(node.get("at_tx"), TransactionKey.class), currentTime);
                }
                throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-basis"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            } else {
                throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-basis"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-basis"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}
