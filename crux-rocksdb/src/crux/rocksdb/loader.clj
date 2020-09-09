(ns ^{:clojure.tools.namespace.repl/load false}
    crux.rocksdb.loader
  (:require [clojure.java.io :as io]
            [crux.io :as cio])
  (:import org.rocksdb.util.Environment
           org.rocksdb.NativeLibraryLoader))

(defn- load-rocksdb-native-lib []
  (let [tmp (cio/create-tmpdir "crux_rocksdb")
        library (io/file tmp (Environment/getJniLibraryFileName "rocksdb"))]
    (io/make-parents library)
    (.loadLibrary (NativeLibraryLoader/getInstance) (str tmp))
    (assert (or (cio/native-image?) *compile-files* (.exists library)))
    (str library)))

(defonce rocksdb-library-path (load-rocksdb-native-lib))
