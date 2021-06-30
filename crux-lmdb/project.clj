(defproject pro.juxt.crux/crux-lmdb "crux-git-version"
  :description "Crux LMDB"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]
                 [pro.juxt.crux/crux-core "crux-git-version"]
                 [com.github.jnr/jnr-ffi "2.1.9"]
                 [org.lmdbjava/lmdbjava "0.7.0" :exclusions [com.github.jnr/jffi]]
                 [org.lwjgl/lwjgl "3.2.3" :classifier "natives-linux" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.2.3" :classifier "natives-linux" :native-prefix ""]
                 [org.lwjgl/lwjgl "3.2.3" :classifier "natives-macos" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.2.3" :classifier "natives-macos" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.2.3"]]
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
