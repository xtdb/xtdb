package crux.api.alpha;

import clojure.lang.Keyword;
import crux.api.Crux;
import crux.api.IndexVersionOutOfSyncException;

import java.util.Map;

abstract class EdnTopology implements Topology {
    abstract Map<Keyword, ?> toEdn();

    @Override
    public final CruxNode startNode() throws IndexVersionOutOfSyncException {
        return new CruxNode(Crux.startNode(toEdn()));
    }
}
