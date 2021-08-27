package xtdb.docs;

// tag::imports[]
import xtdb.api.IXtdb;
// end::imports[]
import java.io.IOException;

public class Install {

    // tag::main-0[]
    public static void main(String[] args) {
        try(IXtdb xtdb = IXtdb.startNode()) {
            // ...
    // end::main-0[]
            // -Werror doesn't like it when the auto-closeable isn't referenced.
            xtdb.status();
    // tag::main-1[]
        }
        catch (IOException e) {
            // ...
        }
    }
    // end::main-1[]
}
