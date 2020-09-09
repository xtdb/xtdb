package docs;

// tag::imports[]
import crux.api.Crux;
import crux.api.ICruxAPI;
// end::imports[]

public class Install {

    // tag::main[]
    public static void main(String[] args) {
        try(ICruxAPI cruxNode = Crux.startNode()) {
            // ...
        }
    }
    // end::main[]
}
