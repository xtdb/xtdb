package crux.api.v2;

import clojure.lang.Keyword;

import java.util.Objects;

public class Attribute {
    private final Keyword kw;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Attribute attribute = (Attribute) o;
        return kw.equals(attribute.kw);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kw);
    }

    @Override
    public String toString() {
        return kw.toString();
    }

    private Attribute(Keyword kw) {
        this.kw = kw;
    }

    public static Attribute attr(String name) {
        return new Attribute(Keyword.intern(name));
    }

    public static Attribute attr(Keyword keyword) {
        return new Attribute(keyword);
    }

    public Keyword toEdn() {
        return kw;
    }
}
