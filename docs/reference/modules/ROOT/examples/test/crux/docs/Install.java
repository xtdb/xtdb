package crux.docs;

// tag::imports[]
import crux.api.Crux;
import crux.api.ICruxAPI;
// end::imports[]
import java.io.IOException;

public class Install {

    // tag::main-0[]
    public static void main(String[] args) {
        try(ICruxAPI cruxNode = Crux.startNode()) {
            // ...
    // end::main-0[]
            // -Werror doesn't like it when the auto-closeable isn't referenced.
            cruxNode.status();
    // tag::main-1[]
        }
        catch (IOException e) {
            // ...
        }
    }
    // end::main-1[]
}
