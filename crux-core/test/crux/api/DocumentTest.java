package crux.api;

import clojure.lang.*;
import crux.api.document.CruxDocument;
import crux.api.document.ICruxDocument;
import crux.api.exception.CruxDocumentException;
import crux.api.exception.CruxIdException;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DocumentTest {
    static private class MyDocument implements ICruxDocument {
        private final Object id;
        private final String name;
        private final String lastName;

        private MyDocument(Object id, String name, String lastName) {
            this.id = id;
            this.name = name;
            this.lastName = lastName;
        }

        @Override
        public Object getDocumentId() {
            return id;
        }

        @Override
        public Map<String, Object> getDocumentContents() {
            HashMap<String, Object> ret = new HashMap<>();
            ret.put("person/name", name);
            ret.put("person/last-name", lastName);
            return ret;
        }
    }

    static private class MyNaughtyDocument implements ICruxDocument {
        String id;
        String notId;

        private MyNaughtyDocument(String id, String notId) {
            this.id = id;
            this.notId = notId;
        }

        @Override
        public Object getDocumentId() {
            return id;
        }

        @Override
        public Map<String, Object> getDocumentContents() {
            HashMap<String, Object> ret = new HashMap<>();
            ret.put("crux.db/id", notId);
            return ret;
        }
    }

    private Keyword key(String in) {
        return Keyword.intern(in);
    }

    private MyDocument createMyDocument(Object id) {
        return new MyDocument(id, "Pablo", "Picasso");
    }

    private IPersistentMap createComparison(Object id) {
        HashMap<Keyword, Object> compareHash = new HashMap<>();
        compareHash.put(key("crux.db/id"), id);
        compareHash.put(key("person/name"), "Pablo");
        compareHash.put(key("person/last-name"), "Picasso");
        return PersistentArrayMap.create(compareHash);
    }

    private void assertIdAllowed(Object id) {
        MyDocument myDocument = createMyDocument(id);
        IPersistentMap compare = createComparison(id);
        Assert.assertEquals(compare, myDocument.toEdn());
    }

    @Test
    public void stringsAreValidIds() {
        assertIdAllowed("PabloPicasso");
    }

    @Test
    public void keywordsAreValidIds() {
        assertIdAllowed(key("PabloPicasso"));
    }

    @Test
    public void integersAreValidIds() {
        assertIdAllowed(4);
    }

    @Test
    public void longsAreValidIds() {
        assertIdAllowed(50L);
    }

    @Test
    public void uuidsAreValidIds() {
        assertIdAllowed(UUID.randomUUID());
    }

    @Test
    public void urisAreValidIds() {
        try {
            assertIdAllowed(new URI("mailto:crux@juxt.pro"));
        }
        catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void urlsAreValidIds() {
        try {
            assertIdAllowed(new URL("https://github.com/juxt/crux"));
        }
        catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void mapsAreValidIds() {
        HashMap<String, String> myId = new HashMap<>();
        myId.put("foo", "bar");
        assertIdAllowed(PersistentArrayMap.create(myId));
    }

    @Test(expected = CruxIdException.class)
    public void otherIdsThrow() {
        IPersistentVector myId = PersistentVector.create("foo", "bar");
        MyDocument myDocument = createMyDocument(myId);
        myDocument.toEdn();
    }

    @Test(expected = CruxDocumentException.class)
    public void tryingToOverwriteCruxDbId() {
        MyNaughtyDocument myDocument = new MyNaughtyDocument("foo", "bar");
        myDocument.toEdn();
    }

    @Test
    public void rebuildingDocuments() {
        MyDocument myDocument = createMyDocument("PabloPicasso");
        IPersistentMap myMap = myDocument.toEdn();
        CruxDocument built = CruxDocument.factory(myMap);
        IPersistentMap decoded = built.toEdn();
        Assert.assertEquals(myMap, decoded);
    }
}
