package crux.api.alpha;

import crux.api.IndexVersionOutOfSyncException;

public interface Topology {
    /**
     * Starts a CruxNode with the current topology options.
     * @return A running instance of CruxNode.
     * @throws IndexVersionOutOfSyncException Thrown if the index needs rebuilding.
     * @see CruxNode
     */
    CruxNode startNode() throws IndexVersionOutOfSyncException;
}
