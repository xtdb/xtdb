(defproject com.xtdb/xtdb-lmdb "<inherited>"
  :description "XTDB LMDB"

  :plugins [[lein-parent "0.3.8"]]

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
                 [org.lwjgl/lwjgl-lmdb "3.3.1"]])
