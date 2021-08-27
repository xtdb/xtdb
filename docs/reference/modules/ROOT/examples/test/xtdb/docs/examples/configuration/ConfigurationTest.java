package xtdb.docs.examples.configuration;

import org.junit.*;
import static org.junit.Assert.*;
import java.io.IOException;
import java.io.File;
import java.net.URL;
import java.net.URISyntaxException;

// tag::import[]
import xtdb.api.IXtdb;
// end::import[]

public class ConfigurationTest {

    @Test
    public void fromFile() {
        // tag::from-file[]
        IXtdb xtdbNode = IXtdb.startNode(new File("resources/config.json"));
        // end::from-file[]

        close(xtdbNode);
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
        IXtdb xtdbNode = IXtdb.startNode(MyApp.class.getResource("config.json"));
        // end::from-resource[]

        close(xtdbNode);
    }

    @Test
    public void withConfigurator() {
        // tag::from-configurator[]
        IXtdb xtdbNode = IXtdb.startNode(n -> {
            // ...
        });
        // end::from-configurator[]

        close(xtdbNode);
    }

    @Test
    public void httpServer() {
        // tag::http-server[]
        IXtdb xtdbNode = IXtdb.startNode(n -> {
            n.with("xtdb.http-server/server", http -> {
                http.set("port", 3000);
            });
        });
        // end::http-server[]

        close(xtdbNode);
    }

    // Not testing this one as it requires real information
    public void overrideModuleImplementation() {
        // tag::override-module[]
        IXtdb xtdbNode = IXtdb.startNode(n -> {
            n.with("xt/document-store", docStore -> {
                docStore.module("xtdb.s3/->document-store");
                docStore.set("bucket", "my-bucket");
                docStore.set("prefix", "my-prefix");
            });
        });
        // end::override-module[]

        close(xtdbNode);
    }

    @Test
    public void nestedModules() {
        // tag::nested-modules-0[]
        IXtdb xtdbNode = IXtdb.startNode(n -> {
            n.with("xt/tx-log", txLog -> {
                txLog.with("kv-store", kv -> {
                    kv.module("xtdb.rocksdb/->kv-store");
                    kv.set("db-dir", new File("/tmp/rocksdb"));
                });
            });
            // end::nested-modules-0[]
            /*
            This obviously won't compile so has to be commented out
            // tag::nested-modules-1[]
            n.with("xt/document-store", docStore -> { ... });
            n.with("xt/index-store", indexStore -> { ... });
            // end::nested-modules-1[]
             */
            // tag::nested-modules-2[]
        });
        // end::nested-modules-2[]

        close(xtdbNode);
    }

    @Test
    public void sharingModules() {
        // tag::sharing-modules[]
        IXtdb xtdbNode = IXtdb.startNode(n -> {
            n.with("my-rocksdb", rocks -> {
                rocks.module("xtdb.rocksdb/->kv-store");
                rocks.set("db-dir", new File("/tmp/rocksdb"));
            });
            n.with("xt/document-store", docStore -> {
                docStore.with("kv-store", "my-rocksdb");
            });
            n.with("xt/tx-log", txLog -> {
                txLog.with("kv-store", "my-rocksdb");
            });
        });
        // end::sharing-modules[]

        close(xtdbNode);
    }

    private void close(IXtdb xtdbNode) {
        try {
            xtdbNode.close();
        }
        catch (IOException e) {
            fail();
        }
    }
}
