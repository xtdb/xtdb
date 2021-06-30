(defproject pro.juxt.crux/crux-lucene "crux-git-version"
  :description "Crux Lucene integration"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../project.clj"
                   :inherit [:repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

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
             "-Dclojure.spec.check-asserts=true"])
