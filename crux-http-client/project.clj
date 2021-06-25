(defproject pro.juxt.crux/crux-http-client "crux-git-version"
  :description "Crux HTTP Client"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [pro.juxt.crux/crux-core "crux-git-version"]
                 [pro.juxt.clojars-mirrors.clj-http/clj-http "3.12.2"]
                 [com.nimbusds/nimbus-jose-jwt "9.7"]]

  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :test {:dependencies [[pro.juxt.crux/crux-test "crux-git-version"]
                                   [pro.juxt.crux/crux-http-server "crux-git-version"]

                                   ;; dependency conflicts
                                   [commons-codec "1.15"]]}}
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
