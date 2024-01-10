package xtdb.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import xtdb.api.tx.TxOp;
import xtdb.api.tx.TxRequest;
import xtdb.api.tx.TxOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TxRequestDeserializerTest {

    private final ObjectMapper objectMapper;

    public TxRequestDeserializerTest() {
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
        Object actual = objectMapper.readValue(json, TxRequest.class);

        // then
        ArrayList<TxOp> ops = new ArrayList<TxOp>();
        ops.add(TxOp.put("docs", Collections.emptyMap()));
        assertEquals(new TxRequest(ops, new TxOptions()), actual);
    }
}
