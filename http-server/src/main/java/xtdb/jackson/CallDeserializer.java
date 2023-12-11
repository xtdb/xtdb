package xtdb.jackson;

import clojure.lang.Keyword;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.tx.Ops;
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
        ObjectNode node = mapper.readTree(p);

        Call op = new Call(mapper.convertValue(node.get("call"), Object.class), mapper.treeToValue(node.get("args"), List.class));

        return op;
    }
}
