(defproject juxt/crux :derived-from-git
  :description "An open source document database with bitemporal graph queries"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :middleware [leiningen.project-version/middleware]
  :plugins [[lein-sub "0.3.0"]]
  :sub ["crux-core"
        "crux-rdf"
        "crux-rocksdb"
        "crux-lmdb"
        "crux-kafka-embedded"
        "crux-kafka"
        "crux-http-client"
        "crux-http-server"
        "crux-uberjar"
        "crux-decorators"
        "crux-test"]
  :aliases {"build" ["do" ["sub" "install"] ["sub" "test"] "test"]}
  :repositories [["snapshots" {:url "https://repo.clojars.org"
                               :username :env/clojars_username
                               :password :env/clojars_password
                               :creds :gpg}]])
