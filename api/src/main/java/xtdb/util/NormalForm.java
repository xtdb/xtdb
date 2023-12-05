package xtdb.util;

import clojure.lang.Keyword;
import clojure.lang.Symbol;

import java.util.Locale;

public class NormalForm {
    private NormalForm() {
    }

    public static String snakeCase(String s) {
        var i = s.lastIndexOf('$');
        if (i < 0) {
            return s.toLowerCase(Locale.ROOT)
                    .replace("$", "/")
                    .replace("-", "_");
        } else {
            return String.format("%s/%s",
                    s.substring(0, i).replace("$", ".").replace("-", "_"),
                    s.substring(i + 1).replace("-", "_"));
        }
    }

    private static String normalise(String s) {
        return s.toLowerCase(Locale.ROOT)
                .replace('.', '$')
                .replace('-', '_');
    }

    public static String normalForm(String s) {
        var i = s.lastIndexOf('/');
        if (i < 0) {
            return normalise(s);
        } else {
            return String.format("%s$%s", normalise(s.substring(0, i)), normalise(s.substring(i + 1)));
        }
    }

    public static Symbol normalForm(Symbol sym) {
        if (sym.getNamespace() != null) {
            return Symbol.intern(String.format("%s$%s", normalise(sym.getNamespace()), normalise(sym.getName())));
        } else {
            return Symbol.intern(normalise(sym.getName()));
        }
    }

    public static Keyword normalForm(Keyword k) {
        return Keyword.intern(normalForm(k.sym));
    }

}
