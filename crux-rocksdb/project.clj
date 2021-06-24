(defproject pro.juxt.crux/crux-rocksdb "crux-git-version-beta"
  :description "Crux RocksDB"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [pro.juxt.crux/crux-core "crux-git-version-beta"]
                 [pro.juxt.crux/crux-metrics "crux-git-version-alpha" :scope "provided"]
                 [org.rocksdb/rocksdbjni "6.12.7"]
                 [com.github.jnr/jnr-ffi "2.1.12"]]
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
