package xtdb.api;

import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static xtdb.api.HttpServer.httpServer;
import static xtdb.api.query.Queries.from;
import static xtdb.api.tx.TxOps.putDocs;

public class XtdbClientJavaTest {
    @Test
    void testClient() {
        try (@SuppressWarnings("unused")
             var server = Xtdb.configure().modules(httpServer()).open();

             var client = XtdbClient.openClient(new URL("http://localhost:3000"))) {

            client.submitTx(putDocs("foo", Map.of("xt/id", "jms")));

            try (var res = client.openQuery(
                from("foo").bind("xt/id", "id").build())) {

                assertEquals(List.of(Map.of("id", "jms")), res.toList());
            }

        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
