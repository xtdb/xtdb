package xtdb.types;

import java.util.Objects;

public record ClojureForm(Object form) {

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (!(other instanceof ClojureForm)) {
            return false;
        }
        ClojureForm cljForm = (ClojureForm) other;
        if (cljForm.form.equals(this.form)) {
            return true;
        }
        return false;
    }

    @Override
        public int hashCode() {
        return Objects.hash(form);
    }
}
