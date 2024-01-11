package xtdb.jackson;

import clojure.java.api.Clojure;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.PersistentHashSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import jsonista.jackson.KeywordKeyDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import xtdb.IllegalArgumentException;
import xtdb.RuntimeException;

import java.io.IOException;
import java.security.Key;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonLdModuleTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JsonLdModule())
                .registerModule(new SimpleModule().addKeyDeserializer(Object.class, new KeywordKeyDeserializer()));
    }

    @Test
    public void shouldDeserializeKeyword() throws IOException {
        String json = """
                { "@type":"xt:keyword", "@value":"foo" }
                """;

        assertEquals(Keyword.intern("foo"), objectMapper.readValue(json, Object.class));
    }

    @Test
    public void shouldDeserializeSet() throws IOException {
        String json = """
                {
                  "@type":"xt:set",
                  "@value": [
                    {"@type":"xt:keyword","@value":"foo"},
                    {"@type":"xt:keyword","@value":"toto"}
                  ]
                }
                """;

        assertEquals(
                PersistentHashSet.create(Keyword.intern("foo"), Keyword.intern("toto")),
                objectMapper.readValue(json, Object.class));
    }

    @Test
    public void shouldDeserializeStandardPersistentMap() throws IOException {
        String json = """
                {"foo":"bar"}
                """;

        assertEquals(
                PersistentHashMap.create(Keyword.intern("foo"), "bar"),
                objectMapper.readValue(json, Object.class));
    }

    @Test
    void shouldDeserializeIllegalArgException() throws IOException {
        String json = """
                {
                  "@type": "xt:error",
                  "@value": {
                    "xtdb.error/message": "sort your request out!",
                    "xtdb.error/class": "xtdb.IllegalArgumentException",
                    "xtdb.error/error-key": "xtdb/malformed-req",
                    "xtdb.error/data": { "a": 1 }}
                  }
                }
                """;

        System.out.println(objectMapper.readValue(json, Object.class));

        assertEquals(
                new IllegalArgumentException(
                        Keyword.intern("xtdb", "malformed-req"),
                        "sort your request out!",
                        PersistentHashMap.create(Keyword.intern("a"), 1),
                        null),
                objectMapper.readValue(json, Object.class));
    }

    @Test
    void shouldDeserializeRuntimeException() throws IOException {
        String json = """
                {
                  "@type": "xt:error",
                  "@value": {
                    "xtdb.error/message": "ruh roh.",
                    "xtdb.error/class": "xtdb.RuntimeException",
                    "xtdb.error/error-key": "xtdb/boom",
                    "xtdb.error/data": { "a": 1 }}
                  }
                }
                """;

        assertEquals(
                new RuntimeException(
                        Keyword.intern("xtdb", "boom"),
                        "ruh roh.",
                        PersistentHashMap.create(Keyword.intern("a"), 1),
                        null),
                objectMapper.readValue(json, Object.class));
    }
}
