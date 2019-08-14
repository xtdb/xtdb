(defproject juxt/crux-rdf "derived-from-git"
  :description "Crux RDF"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "derived-from-git"]
                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "2.5.4"]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "2.5.4"]
                 [ring/ring-core "1.7.1" :scope "provided"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}
  :middleware [leiningen.project-version/middleware])
