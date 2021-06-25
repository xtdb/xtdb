(defproject pro.juxt.crux/crux-lucene "crux-git-version"
  :description "Crux Lucene integration"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [pro.juxt.crux/crux-core "crux-git-version"]

                 [org.apache.lucene/lucene-core "8.6.1"]
                 [org.apache.lucene/lucene-queryparser "8.6.1"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                                  [pro.juxt.crux/crux-test "crux-git-version"]
                                  [pro.juxt.crux/crux-rocksdb "crux-git-version"]]}}
  :middleware [leiningen.project-version/middleware]
  :jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"
             "-Dclojure.spec.compile-asserts=true"
             "-Dclojure.spec.check-asserts=true"]
  :global-vars {*warn-on-reflection* true}
  :pedantic? :warn

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]])

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :creds :gpg}})
