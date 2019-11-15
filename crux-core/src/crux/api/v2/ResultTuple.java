package crux.api.v2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static crux.api.v2.LogicVar.logicVar;

public class ResultTuple {
    private Map<LogicVar, Object> results;
    private List<?> resultArray;

    private ResultTuple(List<LogicVar> logicVars, List<?> queryResult) {
        resultArray = queryResult;
        results = new HashMap<>();

        for (int i = 0; i < logicVars.size(); i++) {
            results.put(logicVars.get(i), queryResult.get(i));
        }
    }

    static ResultTuple resultTuple(List<LogicVar> logicVars, List<?> queryResult) {
        return new ResultTuple(logicVars, queryResult);
    }

    public Object get(LogicVar lv) {
        return results.get(lv);

    }

    public Object get(String key) {
        return get(LogicVar.logicVar(key));
    }

    public Object get(int idx) {
        return resultArray.get(idx);
    }

}
