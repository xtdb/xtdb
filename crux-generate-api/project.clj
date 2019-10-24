(defproject juxt/crux-generate-api "derived-from-git"
  :description "Crux Generate Api"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux-core "derived-from-git"]
                 [juxt/crux-kafka "derived-from-git"]
                 [juxt/crux-jdbc "derived-from-git"]]
  :middleware [leiningen.project-version/middleware]
  :java-source-paths ["src"]
  :javac-options ["-source" "8" "-target" "8"
                  "-Xlint:all,-options,-path"
                  "-Werror"
                  "-proc:none"])
