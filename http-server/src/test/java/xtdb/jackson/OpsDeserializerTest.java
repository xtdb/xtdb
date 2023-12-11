package xtdb.jackson;

import clojure.lang.Keyword;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;
import xtdb.tx.Ops;
import xtdb.tx.Put;
import xtdb.tx.Delete;
import xtdb.tx.Erase;
import xtdb.tx.Call;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OpsDeserializerTest {
    private final ObjectMapper objectMapper;

    public OpsDeserializerTest() {
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule("StandardModule");

        HashMap<Class<?>, JsonDeserializer<?>> deserializerMapping = new HashMap<>();
        deserializerMapping.put(Put.class, new PutDeserializer());
        deserializerMapping.put(Delete.class, new DeleteDeserializer());
        deserializerMapping.put(Erase.class, new EraseDeserializer());
        deserializerMapping.put(Call.class, new CallDeserializer());
        deserializerMapping.put(Ops.class, new OpsDeserializer());
        SimpleDeserializers deserializers = new SimpleDeserializers(deserializerMapping);
        module.setDeserializers(deserializers);

        objectMapper.registerModule(module);
        this.objectMapper = objectMapper;
    }

    @Test
    public void testPutEquals() throws IOException {
        // given
        String put = """
                {"put": "docs", "doc": {}}
                """;

        // when
        Object actual = objectMapper.readValue(put, Ops.class);

        // then
        assertEquals( Ops.put(Keyword.intern("docs"), Collections.emptyMap()), actual);
    }

    @Test
    public void testDeleteEquals() throws IOException {
        // given
        String delete = """
                {"delete": "docs", "id": "my-id"}
                """;

        // when
        Object actual = objectMapper.readValue(delete, Ops.class);

        // then
        assertEquals( Ops.delete(Keyword.intern("docs"), "my-id"), actual);
    }

    @Test
    public void testEraseEquals() throws IOException {
        // given
        String erase = """
                {"erase": "docs", "id": "my-id"}
                """;

        // when
        Object actual = objectMapper.readValue(erase, Ops.class);

        // then
        assertEquals( Ops.erase(Keyword.intern("docs"), "my-id"), actual);
    }

    @Test
    public void testCallEquals() throws IOException {
        // given
        String call = """
                {"call": "my-fn", "args": ["arg1"]}
                """;

        // when
        Object actual = objectMapper.readValue(call, Ops.class);

        // then
        assertEquals( Ops.call("my-fn", List.of("arg1")), actual);
    }
}