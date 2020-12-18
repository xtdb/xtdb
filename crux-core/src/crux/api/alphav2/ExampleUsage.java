package crux.api.alphav2;

import crux.api.Crux;
import crux.api.ICruxAPI;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class ExampleUsage {
    static void main(String... args) {
        javaBuilderStyle();
        usingConsumersNested();
        usingConsumersIndividually();
        submittingTxNatively();
    }

    static void javaBuilderStyle() {
        ModuleConfiguration baz = new ModuleConfiguration.Builder().set("key", "baz").module("waka").build();
        ModuleConfiguration bar = new ModuleConfiguration.Builder().set("key", "bar").with("foo", baz).build();
        NodeConfiguration foo = new NodeConfiguration.Builder().with("key", bar).build();

        try (ICruxAPI node = Crux.startNode(foo)) {
            System.out.println("Used an old Java Builder style");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void usingConsumersNested() {
        try (ICruxAPI node = Crux.startNode(foo -> {
            foo.with("key", bar -> {
                bar.set("key", "bar");
                bar.with("foo", baz -> {
                    baz.set("key", "baz");
                    baz.module("waka");
                });
            });
        })) {
            System.out.println("Used a nested consumer style");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void usingConsumersIndividually() {
        ModuleConfiguration baz = ModuleConfiguration.build( b -> {
           b.set("key", "baz");
           b.module("waka");
        });

        ModuleConfiguration bar = ModuleConfiguration.build( b -> {
            b.set("key", "baz");
            b.with("foo", baz);
        });

        NodeConfiguration foo = NodeConfiguration.build( n -> {
            n.with("key", bar);
        });

        try (ICruxAPI node = Crux.startNode(foo)) {
            System.out.println("Used consumers individually");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class Person implements ICruxDocument {
        private final CruxId id;
        private final String name;
        private final String lastName;

        public Person(String name, String lastName) {
            this.id = CruxId.cruxId(UUID.randomUUID());
            this.name = name;
            this.lastName = lastName;
        }

        @Override
        public CruxId getDocumentId() {
            return id;
        }

        @Override
        public Map<String, Object> getDocumentContents() {
            HashMap<String, Object> map = new HashMap<>();
            map.put("person/name", name);
            map.put("person/lastName", lastName);
            return map;
        }
    }

    static void submittingTxNatively() {
        ICruxAPI node = Crux.startNode();

        node.submitTx( tx -> {
            tx.put(new Person("Ali", "O'Neill"), Date.from(Instant.EPOCH));
            tx.put(new Person("Alistair", "O'Neill"));
            tx.put(new Person("James", "Henderson"));
        });

        node.sync(Duration.ofSeconds(100));

        System.out.println("Done my submitting");
    }
}
