(defproject com.xtdb/xtdb-lmdb "<inherited>"
  :description "XTDB LMDB"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure]
                 [org.clojure/tools.logging]
                 [com.xtdb/xtdb-core]
                 [org.lmdbjava/lmdbjava "0.7.0"]
                 [org.lwjgl/lwjgl "3.2.3" :classifier "natives-linux" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.2.3" :classifier "natives-linux" :native-prefix ""]
                 [org.lwjgl/lwjgl "3.2.3" :classifier "natives-macos" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.2.3" :classifier "natives-macos" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.2.3"]])
