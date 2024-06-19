(ns ^{:clojure.tools.namespace.repl/load false}
    xtdb.rocksdb.loader
  (:import org.rocksdb.NativeLibraryLoader))

(defonce ^:private load-rocksdb
  (.loadLibrary (NativeLibraryLoader/getInstance) nil))
