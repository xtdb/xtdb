package xtdb.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import xtdb.jackson.XtdbMapper;

import static org.junit.jupiter.api.Assertions.*;

class ExprDeserializerTest {
    @Test
    void shouldDeserializeNull() throws JsonProcessingException {
        assertEquals(Expr.NULL, XtdbMapper.QUERY_MAPPER.readValue("null", Expr.class));
    }
}
