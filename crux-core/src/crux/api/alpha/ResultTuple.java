package crux.api.alpha;

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

    /**
     * Gets a value corresponding to a particular logical variable in the result.
     * @param key A string representing the logical variable
     * @return Value at logical variable
     */
    public Object get(String key) {
        return get(Util.symbol(key));
    }

    /**
     * Gets a value at a particular index in the result.
     * @param idx Index to retrieve value from
     * @return Value at index.
     */
    public Object get(int idx) {
        return resultArray.get(idx);
    }

}
