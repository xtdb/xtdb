package xtdb.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import xtdb.api.tx.TxOp;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TxOpDeserializerTest {
    private final ObjectMapper objectMapper;

    public TxOpDeserializerTest() {
        this.objectMapper = XtdbMapper.TX_OP_MAPPER;
    }

    @Test
    public void testPutEquals() throws IOException {
        // given
        String put = """
                {"put": "docs", "doc": {}}
                """;

        // when
        Object actual = objectMapper.readValue(put, TxOp.class);

        // then
        assertEquals( TxOp.put("docs", Collections.emptyMap()), actual);
    }

    @Test
    public void testDeleteEquals() throws IOException {
        // given
        String delete = """
                {"delete": "docs", "id": "my-id"}
                """;

        // when
        Object actual = objectMapper.readValue(delete, TxOp.class);

        // then
        assertEquals( TxOp.delete("docs", "my-id"), actual);
    }

    @Test
    public void testEraseEquals() throws IOException {
        // given
        String erase = """
                {"erase": "docs", "id": "my-id"}
                """;

        // when
        Object actual = objectMapper.readValue(erase, TxOp.class);

        // then
        assertEquals( TxOp.erase("docs", "my-id"), actual);
    }

    @Test
    public void testCallEquals() throws IOException {
        // given
        String call = """
                {"call": "my-fn", "args": ["arg1"]}
                """;

        // when
        Object actual = objectMapper.readValue(call, TxOp.class);

        // then
        assertEquals( TxOp.call("my-fn", List.of("arg1")), actual);
    }

    @Test
    public void testSqlEquals() throws IOException {
        // given
        String sql = """
                {"sql": "INSERT INTO docs (xt$id, foo) VALUES (?, ?)",
                 "arg_rows": [[1, "foo"], [2, "bar"]]}
                """;

        // when
        Object actual = objectMapper.readValue(sql, TxOp.class);

        // then
        assertEquals(TxOp.sqlBatch("INSERT INTO docs (xt$id, foo) VALUES (?, ?)", List.of(List.of(1L, "foo"), List.of(2L, "bar"))), actual);
    }
}
