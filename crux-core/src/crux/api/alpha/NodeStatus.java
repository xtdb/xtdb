package crux.api.alpha;

import clojure.lang.Keyword;

import java.util.Map;

public class NodeStatus {
    public final String version;
    public final String revision;
    public final String kvStore;
    public final int estimatedNumberOfKeys;
    public final long size;
    public final long indexVersion;

    @Override
    public String toString() {
        return "NodeStatus{" +
            "version='" + version + '\'' +
            ", revision='" + revision + '\'' +
            ", kvStore='" + kvStore + '\'' +
            ", estimatedNumberOfKeys=" + estimatedNumberOfKeys +
            ", size=" + size +
            ", indexVersion=" + indexVersion +
            '}';
    }

    private NodeStatus(String version, String revision, String kvStore, int estimatedNumberOfKeys, long size, long indexVersion) {
        this.version = version;
        this.revision = revision;
        this.kvStore = kvStore;
        this.estimatedNumberOfKeys = estimatedNumberOfKeys;
        this.size = size;
        this.indexVersion = indexVersion;
    }

    @SuppressWarnings("unchecked")
    static NodeStatus nodeStatus(Map<Keyword,?> status) {
        String version = (String) status.get(Util.keyword("crux.version/version" ));
        String revision = (String) status.get(Util.keyword("crux.version/revision" ));
        String kvStore = (String) status.get(Util.keyword("crux.kv/kv-store" ));
        Integer estimatedNumberOfKeys = (Integer) status.get(Util.keyword("crux.kv/estimate-num-keys" ));
        Long size = (Long) status.get(Util.keyword("crux.kv/size" ));
        Long indexVersion = (Long) status.get(Util.keyword("crux.index/index-version" ));
        // TODO: Add 'crux.tx-log/consumer-state'
        return new NodeStatus(version, revision, kvStore, estimatedNumberOfKeys, size, indexVersion);
    }
}
