(defproject juxt/crux-lucene "crux-git-version-alpha"
  :description "Crux Lucene integration"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "crux-git-version-beta"]

                 [org.apache.lucene/lucene-core "8.6.1"]
                 [org.apache.lucene/lucene-queryparser "8.6.1"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                                  [juxt/crux-test "crux-git-version"]
                                  [juxt/crux-rocksdb "crux-git-version-beta"]]}}
  :middleware [leiningen.project-version/middleware]
  :jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"
             "-Dclojure.spec.compile-asserts=true"
             "-Dclojure.spec.check-asserts=true"]
  :global-vars {*warn-on-reflection* true}
  :pedantic? :warn)
