package crux.api.v2;

import clojure.lang.Keyword;

import java.util.Map;

public abstract class Topology {
    protected abstract Map<Keyword, Object> toEdn();
    public abstract Topology withTopologyMap(Map<Attribute, ?> topologyAttrs);
}
