package xtdb.jackson;

import clojure.lang.Keyword;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import xtdb.tx.Ops;
import xtdb.tx.Sql;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OpsDeserializerTest {
    private final ObjectMapper objectMapper;

    public OpsDeserializerTest() {
        this.objectMapper = XtdbMapper.TX_OP_MAPPER;
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

    @Test
    public void testSqlEquals() throws IOException {
        // given
        String sql = """
                {"sql": "INSERT INTO docs (xt$id, foo) VALUES (?, ?)",
                 "arg_rows": [[1, "foo"], [2, "bar"]]}
                """;

        // when
        Object actual = objectMapper.readValue(sql, Ops.class);

        // then
        assertEquals(Ops.sqlBatch("INSERT INTO docs (xt$id, foo) VALUES (?, ?)", List.of(List.of(1L, "foo"), List.of(2L, "bar"))), actual);
    }
}