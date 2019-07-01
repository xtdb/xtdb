(defproject juxt/crux-rdf :derived-from-git
  :description "Crux RDF"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [juxt/crux-core :derived-from-git]

                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "2.5.1" :scope "provided"]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "2.5.1" :scope "provided"]]
  :profiles {:dev {:dependencies [[juxt/crux-dev :derived-from-git]]}}
  :middleware [leiningen.project-version/middleware])
