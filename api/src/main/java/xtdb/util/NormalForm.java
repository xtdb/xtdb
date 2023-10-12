package xtdb.util;

import clojure.lang.Keyword;
import clojure.lang.Symbol;

import java.util.Locale;

public class NormalForm {
    private NormalForm() {
    }

    // TODO we could cache these? guessing they're probably not free.
    
    private static String normalise(String s) {
        return s.toLowerCase(Locale.ROOT)
                .replace('.', '$')
                .replace('-', '_');
    }

    private static String unnormalise(String s) {
        return s.replace('_', '-')
                .replace('$', '.');
    }

    public static String normalForm(String s) {
        var i = s.lastIndexOf('/');
        if (i < 0) {
            return normalise(s);
        } else {
            return "%s$%s".formatted(normalise(s.substring(0, i)), normalise(s.substring(i + 1)));
        }
    }

    public static String datalogForm(String s) {
        var i = s.lastIndexOf('$');
        if (i < 0) {
            return unnormalise(s);
        } else {
            return "%s/%s".formatted(unnormalise(s.substring(0, i)), unnormalise(s.substring(i + 1)));
        }
    }

    public static Keyword normalForm(Keyword k) {
        return Keyword.intern(normalForm(k.sym));
    }

    public static Keyword datalogForm(Keyword normalFormKeyword) {
        var k = normalFormKeyword.sym.toString();
        var i = k.lastIndexOf('$');
        if (i < 0) {
            return Keyword.intern(unnormalise(k));
        } else {
            return Keyword.intern(unnormalise(k.substring(0, i)), unnormalise(k.substring(i + 1)));
        }
    }

    public static Symbol normalForm(Symbol sym) {
        if (sym.getNamespace() != null) {
            return Symbol.intern("%s$%s".formatted(normalise(sym.getNamespace()), normalise(sym.getName())));
        } else {
            return Symbol.intern(normalise(sym.getName()));
        }
    }
}
