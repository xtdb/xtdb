package crux.api.v2;

import clojure.lang.Keyword;
import crux.api.IndexVersionOutOfSyncException;

import java.util.Map;

public interface Topology {
    CruxNode startNode() throws IndexVersionOutOfSyncException;
}
