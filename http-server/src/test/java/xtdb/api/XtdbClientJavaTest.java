package xtdb.api;

import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static xtdb.api.HttpServer.httpServer;
import static xtdb.api.query.Queries.from;
import static xtdb.api.tx.TxOps.putDocs;
import static xtdb.api.tx.TxOps.sql;

public class XtdbClientJavaTest {
    @Test
    void testClient() {
        try (@SuppressWarnings("unused")
             var server = Xtdb.configure().modules(httpServer()).open();

             var client = XtdbClient.openClient(new URI("http://localhost:3000").toURL())) {

            client.submitTx(sql("INSERT INTO foo (_id) VALUES ('jms')"));

            try (var res = client.openQuery("SELECT _id foo_id FROM foo")) {
                assertEquals(List.of(Map.of("foo_id", "jms")), res.toList());
            }

        } catch (MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
