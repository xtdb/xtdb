package xtdb.jackson;


import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import xtdb.IllegalArgumentException;
import xtdb.tx.Ops;
import xtdb.tx.Put;
import xtdb.tx.Delete;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class OpsDeserializer extends StdDeserializer<Ops>  {

    public OpsDeserializer() {
        super(Ops.class);
    }

    @Override
    public Ops deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        TypeFactory typeFactory = mapper.getTypeFactory();
        ObjectNode node = mapper.readTree(p);

        if (node.has("put")) {
            return mapper.treeToValue(node, Put.class);
        } else if (node.has("delete")) {
            return mapper.treeToValue(node, Delete.class);
        } else {
            // TODO DELETE, ERASE, CALL
           throw new IllegalArgumentException("unsupported", PersistentHashMap.EMPTY, null);
        }
    }
}