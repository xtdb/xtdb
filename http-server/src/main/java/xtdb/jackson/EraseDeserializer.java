package xtdb.jackson;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;
import xtdb.tx.Erase;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

public class EraseDeserializer extends StdDeserializer<Erase> {

    public EraseDeserializer() {
        super(Erase.class);
    }

    @Override
    public Erase deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        BaseJsonNode node = mapper.readTree(p);

        try {
            ObjectNode objectNode = (ObjectNode) node;
            Erase op = new Erase(Keyword.intern(node.get("erase").asText()), mapper.convertValue(node.get("id"), Object.class));
            return op;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-erase"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }  
    }
}
