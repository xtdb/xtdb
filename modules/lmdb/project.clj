(defproject com.xtdb/xtdb-lmdb "<inherited>"
  :description "XTDB LMDB"

  :dependencies [[org.clojure/clojure]
                 [org.clojure/tools.logging]
                 [com.xtdb/xtdb-core]
                 [org.lmdbjava/lmdbjava "0.8.2"]
                 [org.lwjgl/lwjgl "3.3.1" :classifier "natives-linux" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.3.1" :classifier "natives-linux" :native-prefix ""]
                 [org.lwjgl/lwjgl "3.3.1" :classifier "natives-macos" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.3.1" :classifier "natives-macos" :native-prefix ""]
                 [org.lwjgl/lwjgl "3.3.1" :classifier "natives-macos-arm64" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.3.1" :classifier "natives-macos-arm64" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.3.1"]]

  :plugins [[lein-javadoc "0.3.0"]
            [lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :java-source-paths ["src"]

  :javac-options ["-source" "8" "-target" "8"
                  "-XDignore.symbol.file"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"]

  :javadoc-opts {:package-names ["xtdb"]
                 :output-dir "target/javadoc/out"
                 :additional-args ["-windowtitle" "XTDB LMDB Javadoc"
                                   "-quiet"
                                   "-Xdoclint:none"
                                   "-link" "https://docs.oracle.com/javase/8/docs/api/"
                                   "-link" "https://www.javadoc.io/static/org.clojure/clojure/1.10.3"]}

  :classifiers {:sources {:prep-tasks ^:replace []}
                :javadoc {:prep-tasks ^:replace ["javadoc"]
                          :omit-source true
                          :filespecs ^:replace [{:type :path, :path "target/javadoc/out"}]}})
