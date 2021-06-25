(defproject pro.juxt.crux-labs/crux-graal "crux-git-version-alpha"
  :description "Crux Graal"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [ch.qos.logback/logback-classic "1.2.3" :exclusions [org.slf4j/slf4j-api]]
                 [org.slf4j/slf4j-simple "1.7.26"]
                 [pro.juxt.crux/crux-core "crux-git-version-alpha"]]
  :middleware [leiningen.project-version/middleware]

  :jvm-opts ["-Dclojure.compiler.direct-linking=true"
             "-Dclojure.spec.compile-asserts=false"]

  :aot [crux.mem-kv crux.rocksdb crux.main.graal]
  :main crux.main.graal

  :pedantic? :warn

  :pom-addition ([:developers
                  [:developer
                   [:id "juxt"]
                   [:name "JUXT"]]])

  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                                     :creds :gpg}})
