package crux.api.alpha;

import crux.api.IndexVersionOutOfSyncException;

public interface Topology {
    CruxNode startNode() throws IndexVersionOutOfSyncException;
}
