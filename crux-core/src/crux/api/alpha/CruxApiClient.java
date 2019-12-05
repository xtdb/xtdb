package crux.api.alpha;

import crux.api.Crux;
import crux.api.ICruxAPI;

import java.util.Date;

import static crux.api.alpha.Database.database;

public class CruxApiClient extends CruxNode {
    CruxApiClient (ICruxAPI apinode) {
        super(apinode);
    }

    /**
     * Gets a Database instance as of now.
     * @return Database instance with the valid & transaction time set to the current time
     * @see Database
     */
    public Database db() {
        Date currentTime = new Date();
        return db(currentTime, currentTime);
    }

    /**
     * Gets a Database instance as of a valid time.
     * @param validTime The valid time
     * @return Database instance at validTime, with transaction time set to the current time
     * @see Database
     */
    public Database db(Date validTime) {
        Date currentTime = new Date();
        return db(validTime, currentTime);
    }

    /**
     * Gets a Database instance as of a valid and a transaction time. Will block until the transaction time is present in the index.
     * @return Database instance at valid time and transaction time
     * @param validTime The valid time
     * @param transactionTime The transaction time
     * @see Database
     */
    public Database db(Date validTime, Date transactionTime) {
        return database(node, validTime, transactionTime);
    }


    public static CruxApiClient cruxApiClient(String url) {
        ICruxAPI apinode = Crux.newApiClient(url);
        return new CruxApiClient(apinode);
    }
}
