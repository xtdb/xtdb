(def modules ["crux-core"
              "crux-rdf"
              "crux-rocksdb"
              "crux-lmdb"
              "crux-kafka-embedded"
              "crux-kafka"
              "crux-jdbc"
              "crux-http-client"
              "crux-http-server"
              "crux-uberjar"
              "crux-decorators"
              "crux-test"])

(defproject pro.juxt/crux "derived-from-git"
  :description "An open source document database with bitemporal graph queries"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :middleware [leiningen.project-version/middleware]
  :plugins [[lein-sub "0.3.0"]]
  :sub ~modules
  :scm {:name "git" :url "https://github.com/juxt/crux"}
  :pom-addition [:developers [:developer {:id "jdt_juxt"}
                              [:name "Jeremy Taylor"]
                              [:url "https://juxt.pro"]]]
  :aliases {"check" ["sub" "-s" ~(clojure.string/join ":" (remove #{"crux-jdbc"} modules)) "check"]
            "build" ["do" ["sub" "install"] ["sub" "test"]]}
;  :repositories [["snapshots" {:url "https://repo.clojars.org"
;                               :username :env/clojars_username
;                               :password :env/clojars_password
;                               :creds :gpg}]]
  :deploy-repositories [["releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2/"}
                         "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"}
                                     ]])
