package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;
import xtdb.api.TransactionKey;
import xtdb.api.query.Basis;

import java.io.IOException;
import java.time.Instant;

public class BasisDeserializer extends StdDeserializer<Basis> {

    public BasisDeserializer() {
        super(Basis.class);
    }

    public Basis deserialize (JsonParser p, DeserializationContext ctxt) throws IOException{
        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        if (!node.isObject()) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-basis"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        Instant currentTime = null;
        TransactionKey atTx = null;
        if (node.has("current_time")) {
            var candidateCurrentTime = codec.readValue(node.get("current_time").traverse(codec), Object.class);
            if (!(candidateCurrentTime instanceof Instant)) {
                throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-basis"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
            }
            currentTime = (Instant) candidateCurrentTime;
        }
        if (node.has("at_tx")) {
            atTx = codec.treeToValue(node.get("at_tx"), TransactionKey.class);
        }
        return new Basis(atTx, currentTime);
    }
}
