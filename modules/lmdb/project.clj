(defproject com.xtdb/xtdb-lmdb "<inherited>"
  :description "XTDB LMDB"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.1.0"]
                 [com.xtdb/xtdb-core]
                 [com.github.jnr/jnr-ffi "2.1.9"]
                 [org.lmdbjava/lmdbjava "0.7.0" :exclusions [com.github.jnr/jffi]]
                 [org.lwjgl/lwjgl "3.2.3" :classifier "natives-linux" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.2.3" :classifier "natives-linux" :native-prefix ""]
                 [org.lwjgl/lwjgl "3.2.3" :classifier "natives-macos" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.2.3" :classifier "natives-macos" :native-prefix ""]
                 [org.lwjgl/lwjgl-lmdb "3.2.3"]])
