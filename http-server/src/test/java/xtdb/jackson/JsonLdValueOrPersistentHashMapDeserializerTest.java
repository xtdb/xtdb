package xtdb.jackson;

import static org.junit.jupiter.api.Assertions.*;

import clojure.java.api.Clojure;
import clojure.lang.*;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class JsonLdValueOrPersistentHashMapDeserializerTest {

    private final ObjectMapper objectMapper;
    private IFn keywordFn = Clojure.var("clojure.core", "keyword");
    private IFn setFn = Clojure.var("clojure.core", "set");

    public JsonLdValueOrPersistentHashMapDeserializerTest() {
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule("JsonLd-test");

        HashMap<String, IFn> decoders = new HashMap<String, IFn>();
        decoders.put("xt:keyword", keywordFn);
        decoders.put("xt:set", setFn);

        HashMap<Class<?>, JsonDeserializer<?>> deserializerMapping = new HashMap<>();
        deserializerMapping.put(Map.class, new JsonLdValueOrPersistentHashMapDeserializer(decoders));
        SimpleDeserializers deserializers = new SimpleDeserializers(deserializerMapping);
        module.setDeserializers(deserializers);
        objectMapper.registerModule(module);
        this.objectMapper = objectMapper;
    }

    @Test
    public void shouldDeserializeKeyword() throws IOException {
        // given
        String json = """
                    {"@type":"xt:keyword","@value":"foo"}
                    """;

        // when
        Object actual = objectMapper.readValue(json, Object.class);

        // then
        assertEquals(Keyword.intern("foo"), actual);
    }
    @Test
    public void shouldDeserializeSet() throws IOException {
        // given
        String json = """
               {"@type":"xt:set","@value":[{"@type":"xt:keyword","@value":"foo"},{"@type":"xt:keyword","@value":"toto"}]}
               """;

        // when
        Object actual = objectMapper.readValue(json, Object.class);

        // then
        assertEquals(PersistentHashSet.create(Keyword.intern("foo"), Keyword.intern("toto") ), actual);
    }

    @Test
    public void shouldDeserializeStandardPersistentMap() throws IOException {
        // given
        String json = """
                {"foo":"bar"}
                """;

        // when
        Object actual = objectMapper.readValue(json, Object.class);

        // then
        assertEquals(PersistentHashMap.create("foo", "bar"), actual);
    }
}
