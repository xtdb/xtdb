package xtdb.jackson;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;
import xtdb.tx.TxOptions;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;

public class TxOptionsDeserializer extends StdDeserializer<TxOptions> {

    public TxOptionsDeserializer() {
        super(TxOptions.class);
    }

    @Override
    public TxOptions deserialize(com.fasterxml.jackson.core.JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) jp.getCodec();

        ObjectNode node = mapper.readTree(jp);
        Instant systemTime = null;
        ZoneId defaultTz = null;

        try {
            if (node.has("system_time")) {
                systemTime = (Instant) mapper.readValue(node.get("system_time").traverse(mapper), Object.class);
            }
            if (node.has("default_tz")) {
                defaultTz = (ZoneId)  mapper.readValue(node.get("default_tz").traverse(mapper), Object.class);
            }
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-tx-options"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }

        return new TxOptions(systemTime, defaultTz);
    }
}