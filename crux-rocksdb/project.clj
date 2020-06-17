(defproject juxt/crux-rocksdb "crux-git-version-beta"
  :description "Crux RocksDB"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [juxt/crux-metrics "crux-git-version-alpha" :scope "provided"]
                 [org.rocksdb/rocksdbjni "6.8.1"]
                 [com.github.jnr/jnr-ffi "2.1.12"]]
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
