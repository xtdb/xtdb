package xtdb.jackson;

import clojure.lang.Keyword;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import xtdb.tx.Ops;
import xtdb.tx.Tx;
import xtdb.api.TxOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TxDeserializerTest {

    private final ObjectMapper objectMapper;

    public TxDeserializerTest() {
        this.objectMapper = XtdbMapper.TX_OP_MAPPER;
    }

    @Test
    public void shouldDeserializeTx() throws IOException {
        // given
        String json =
                    """
                    {"tx_ops":[{"put":"docs","doc":{}}]}
                """;

        // when
        Object actual = objectMapper.readValue(json, Tx.class);

        // then
        ArrayList<Ops> ops = new ArrayList<Ops>();
        ops.add(Ops.put(Keyword.intern("docs"), Collections.emptyMap()));
        assertEquals(new Tx(ops, new TxOptions()), actual);
    }
}
