package xtdb.api;

import clojure.lang.Keyword;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static xtdb.api.tx.TxOptions.txOpts;
import static xtdb.api.query.Binding.bindVar;
import static xtdb.api.query.Query.from;
import static xtdb.api.tx.TxOp.put;

class IXtdbJavaTest {
    private IXtdb node;

    @BeforeEach
    void setUp() {
        node = Xtdb.startNode();
    }

    @AfterEach
    void tearDown() {
        node.close();
    }

    @Test
    void javaApiTest() {
        node.submitTx(txOpts().systemTime(Instant.parse("2020-01-01T12:34:56.000Z")).build(),
                put("docs", Map.of("xt/id", 1, "foo", "bar")));

        try (var res = node.openQuery(
                from("docs",
                        List.of(bindVar("xt/id"), bindVar("xt/system_from"))))) {
            assertEquals(
                    List.of(Map.of(
                            "xt/id", 1,
                            "xt/system_from", ZonedDateTime.parse("2020-01-01T12:34:56Z[UTC]"))),
                    res.toList());
        }
    }
}
