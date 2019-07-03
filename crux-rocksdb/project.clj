(defproject juxt/crux-rocksdb :derived-from-git
  :description "Crux RocksDB"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux-core :derived-from-git]
                 [org.rocksdb/rocksdbjni "6.0.1"]
                 [com.github.jnr/jnr-ffi "2.1.9"]]
  :middleware [leiningen.project-version/middleware])
