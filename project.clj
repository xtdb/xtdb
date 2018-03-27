(defproject ding "0.1.0-SNAPSHOT"
  :description "A Clojure library that gives graph query over RocksDB"
  :url "https://github.com/juxt/ding"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.taoensso/nippy "2.13.0"]
                 [org.rocksdb/rocksdbjni "5.11.3"]
                 [gloss "0.2.6"]]
  :profiles {:dev {:dependencies [[clj-time "0.14.2"]]}})
