package xtdb.jackson;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import xtdb.IllegalArgumentException;
import xtdb.tx.Call;

import java.io.IOException;
import java.util.List;

public class CallDeserializer extends StdDeserializer<Call> {

    public CallDeserializer() {
        super(Call.class);
    }

    @Override
    public Call deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        BaseJsonNode node = mapper.readTree(p);
        
        try {
            ObjectNode objectNode = (ObjectNode) node;
            Call op = new Call(mapper.convertValue(objectNode.get("call"), Object.class), mapper.treeToValue(objectNode.get("args"), List.class));
            return op;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtdb", "malformed-call"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }  
    }
}
