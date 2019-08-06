(defproject juxt/crux-http-client "derived-from-git"
  :description "Crux HTTP Client"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [clj-http "3.10.0"]

                 [juxt/crux-core "derived-from-git"]]
  :middleware [leiningen.project-version/middleware])
