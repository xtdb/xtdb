package crux.api.v2;

import crux.api.ICruxAPI;
import crux.api.ICruxDatasource;

import java.util.*;

public class Database {
    private final ICruxDatasource db;

    private Database(ICruxDatasource db) {
        this.db = db;
    }

    protected static Database database(ICruxAPI node) {
        return new Database(node.db());
    }

    protected static Database database(ICruxAPI node, Date validTime) {
        return new Database(node.db(validTime));
    }

    protected static Database database(ICruxAPI node, Date validTime, Date transactionTime) {
        return new Database(node.db(validTime, transactionTime));
    }

    public Set<ResultTuple> query(Query query) {
        Collection<List<?>> queryResult = db.q(query.toEdn());
        List<LogicVar> symbols = query.findLogicVars();

        Set resultSet = new HashSet<>();
        for(List<?> result : queryResult){
            resultSet.add(ResultTuple.resultTuple(symbols, result));
        }

        return resultSet;
    }

    public Collection<List<?>> query(String query) {
        return db.q(query);
    }

    public Document entity(Database db, CruxId id) {
        throw new UnsupportedOperationException();
    }
}
