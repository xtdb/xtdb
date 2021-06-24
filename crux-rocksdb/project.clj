(defproject pro.juxt.crux/crux-rocksdb "crux-git-version-beta"
  :description "Crux RocksDB"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [pro.juxt.crux/crux-core "crux-git-version-beta"]
                 [pro.juxt.crux/crux-metrics "crux-git-version-alpha" :scope "provided"]
                 [org.rocksdb/rocksdbjni "6.12.7"]
                 [com.github.jnr/jnr-ffi "2.1.12"]]
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]])

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :creds :gpg}})
