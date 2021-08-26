package crux.api;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.ILookup;
import clojure.lang.Keyword;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.*;
import static crux.api.NodeConfiguration.buildNode;
import static crux.api.ModuleConfiguration.buildModule;

import crux.api.*;

public class ConfigurationTest {
    private static final IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");
    private static final IFn getKvName = (IFn) requiringResolve.invoke(Clojure.read("crux.kv/kv-name"));

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ModuleConfiguration createKvConfig(File folder) {
        return buildModule(m -> m.with("kv-store", buildModule(kv -> {
            kv.module("xtdb.rocksdb/->kv-store");
            kv.set("db-dir", folder);
        })));
    }

    private ICruxIngestAPI startIngestNode() {
        try {
            File docDir = folder.newFolder("docs");
            return Crux.startNode(n -> {
                n.with("xt/document-store", createKvConfig(docDir));
            });
        }
        catch (IOException e) {
            fail();
            return null;
        }
    }

    private Object unwrap(Object object, String keyword) {
        ILookup lookup = (ILookup) object;
        Keyword key = Keyword.intern(keyword);
        return lookup.valAt(key);
    }

    private Object unwrap(Object api, String... keywords) {
        Object item = api;
        for (String keyword: keywords) {
            item = unwrap(item, keyword);
        }
        return item;
    }

    private String kvStore(Object api, String... keywords) {
        return (String) getKvName.invoke(unwrap(api, keywords));
    }

    @Test
    public void canUseRocksOnICruxAPI() {
        try {
            File txDir = folder.newFolder("tx");
            File docDir = folder.newFolder("docs");
            File indexDir = folder.newFolder("index");

            ICruxAPI node = Crux.startNode(n -> {
                n.with("xt/tx-log", createKvConfig(txDir));
                n.with("xt/document-store", createKvConfig(docDir));
                n.with("xt/index-store", createKvConfig(indexDir));
            });

            assertEquals("xtdb.rocksdb.RocksKv", kvStore(node, "node", "tx-log", "kv-store"));
            assertEquals("xtdb.rocksdb.RocksKv", kvStore(node, "node", "document-store", "document-store", "kv-store"));
            assertEquals("xtdb.rocksdb.RocksKv", kvStore(node, "node", "tx-log", "kv-store"));

            assertEquals(txDir.toPath(), unwrap(node, "node", "tx-log", "kv-store", "db-dir"));
        }
        catch (IOException e) {
            fail();
        }
    }

    @Test
    public void canUseRocksOnICruxIngestAPI() {
        try {
            File docDir = folder.newFolder("docs");

            ICruxIngestAPI client = Crux.newIngestClient(n -> {
                n.with("xt/document-store", createKvConfig(docDir));
            });

            assertEquals("xtdb.rocksdb.RocksKv", kvStore(client, "client", "document-store", "document-store", "kv-store"));
        }
        catch (IOException e) {
            fail();
        }
    }

    @Test
    public void consumerAndExplicitBuildersAreEquivalent() {
        ModuleConfiguration internalExplicitModule = ModuleConfiguration.builder()
                .set("foo", "bar")
                .build();

        ModuleConfiguration explicitModule = ModuleConfiguration.builder()
                .set("foo", "bar")
                .with("baz")
                .with("waka", internalExplicitModule)
                .set(Collections.singletonMap("foo2", 3))
                .build();

        NodeConfiguration explicitNode = NodeConfiguration.builder()
                .with("nodeFoo", explicitModule)
                .with("nodeBar")
                .build();

        NodeConfiguration consumerNode = buildNode(n -> {
            n.with("nodeFoo", buildModule(m -> {
                m.set("foo", "bar");
                m.with("baz");
                m.with("waka", m2 -> m2.set("foo", "bar"));
                m.set(Collections.singletonMap("foo2", 3));
            }));
            n.with("nodeBar");
        });

        assertEquals(explicitNode, consumerNode);
    }
}
