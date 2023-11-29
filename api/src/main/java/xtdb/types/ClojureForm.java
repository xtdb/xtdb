package xtdb.types;

import java.util.Objects;

public final class ClojureForm {
    private final Object form;

    public ClojureForm(Object form) {
        this.form = form;
    }

    public Object form() {
        return form;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ClojureForm) obj;
        return Objects.equals(this.form, that.form);
    }

    @Override
    public int hashCode() {
        return Objects.hash(form);
    }

    @Override
    public String toString() {
        return "ClojureForm[" +
                "form=" + form + ']';
    }

}
