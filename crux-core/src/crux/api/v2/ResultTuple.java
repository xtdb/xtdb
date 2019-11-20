package crux.api.v2;

import clojure.lang.Symbol;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultTuple {
    private Map<Symbol, Object> results;
    private List<?> resultArray;

    private ResultTuple(List<Symbol> symbols, List<?> queryResult) {
        resultArray = queryResult;
        results = new HashMap<>();

        for (int i = 0; i < symbols.size(); i++) {
            results.put(symbols.get(i), queryResult.get(i));
        }
    }

    static ResultTuple resultTuple(List<Symbol> symbols, List<?> queryResult) {
        return new ResultTuple(symbols, queryResult);
    }

    public Object get(Symbol lv) {
        return results.get(lv);

    }

    public Object get(String key) {
        return get(Util.sym(key));
    }

    public Object get(int idx) {
        return resultArray.get(idx);
    }

}
