(defproject pro.juxt.crux/crux-rdf "crux-git-version-alpha"
  :description "Crux RDF"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [pro.juxt.crux/crux-core "crux-git-version-beta"]
                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "3.0.0"]
                 [pro.juxt.clojars-mirrors.ring/ring-core "1.9.2" :scope "provided"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
