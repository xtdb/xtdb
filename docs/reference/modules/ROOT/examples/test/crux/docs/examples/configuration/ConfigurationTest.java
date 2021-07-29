package crux.docs.examples.configuration;

import org.junit.*;
import static org.junit.Assert.*;
import java.io.IOException;
import java.io.File;
import java.net.URL;
import java.net.URISyntaxException;

// tag::import[]
import crux.api.Crux;
import crux.api.ICruxAPI;
// end::import[]

public class ConfigurationTest {

    @Test
    public void fromFile() {
        // tag::from-file[]
        ICruxAPI cruxNode = Crux.startNode(new File("resources/config.json"));
        // end::from-file[]

        close(cruxNode);
    }

    @Test
    public void fromResource() {
        try {
            URL url = MyApp.class.getResource("config.json");
            File file = new File(url.toURI());
            assertTrue(file.exists());
        }
        catch (URISyntaxException e) {
            fail();
        }
        // tag::from-resource[]
        ICruxAPI cruxNode = Crux.startNode(MyApp.class.getResource("config.json"));
        // end::from-resource[]

        close(cruxNode);
    }

    @Test
    public void withConfigurator() {
        // tag::from-configurator[]
        ICruxAPI cruxNode = Crux.startNode(n -> {
            // ...
        });
        // end::from-configurator[]

        close(cruxNode);
    }

    @Test
    public void httpServer() {
        // tag::http-server[]
        ICruxAPI cruxNode = Crux.startNode(n -> {
            n.with("crux.http-server/server", http -> {
                http.set("port", 3000);
            });
        });
        // end::http-server[]

        close(cruxNode);
    }

    // Not testing this one as it requires real information
    public void overrideModuleImplementation() {
        // tag::override-module[]
        ICruxAPI cruxNode = Crux.startNode(n -> {
            n.with("crux/document-store", docStore -> {
                docStore.module("crux.s3/->document-store");
                docStore.set("bucket", "my-bucket");
                docStore.set("prefix", "my-prefix");
            });
        });
        // end::override-module[]

        close(cruxNode);
    }

    @Test
    public void nestedModules() {
        // tag::nested-modules-0[]
        ICruxAPI cruxNode = Crux.startNode(n -> {
            n.with("crux/tx-log", txLog -> {
                txLog.with("kv-store", kv -> {
                    kv.module("crux.rocksdb/->kv-store");
                    kv.set("db-dir", new File("/tmp/rocksdb"));
                });
            });
            // end::nested-modules-0[]
            /*
            This obviously won't compile so has to be commented out
            // tag::nested-modules-1[]
            n.with("crux/document-store", docStore -> { ... });
            n.with("crux/index-store", indexStore -> { ... });
            // end::nested-modules-1[]
             */
            // tag::nested-modules-2[]
        });
        // end::nested-modules-2[]

        close(cruxNode);
    }

    @Test
    public void sharingModules() {
        // tag::sharing-modules[]
        ICruxAPI cruxNode = Crux.startNode(n -> {
            n.with("my-rocksdb", rocks -> {
                rocks.module("crux.rocksdb/->kv-store");
                rocks.set("db-dir", new File("/tmp/rocksdb"));
            });
            n.with("crux/document-store", docStore -> {
                docStore.with("kv-store", "my-rocksdb");
            });
            n.with("crux/tx-log", txLog -> {
                txLog.with("kv-store", "my-rocksdb");
            });
        });
        // end::sharing-modules[]

        close(cruxNode);
    }

    private void close(ICruxAPI cruxNode) {
        try {
            cruxNode.close();
        }
        catch (IOException e) {
            fail();
        }
    }
}
