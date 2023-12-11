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
import xtdb.tx.Call;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class CallDeserializer extends StdDeserializer<Call> {

    public CallDeserializer() {
        super(Call.class);
    }

    @Override
    public Call deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        BaseJsonNode node = mapper.readTree(p);
        
        try {
            ObjectNode objectNode = (ObjectNode) node;
            Call op = new Call(mapper.convertValue(node.get("call"), Object.class), mapper.treeToValue(node.get("args"), List.class));
            return op;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-call"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()));
        }  
    }
}
