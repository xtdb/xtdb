(defproject com.xtdb.labs/xtdb-rdf "<inherited>"
  :description "XTDB RDF"

  :plugins [[lein-parent "0.3.8"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure]
                 [com.xtdb/xtdb-core]
                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "3.0.0"]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "3.0.0"]
                 [pro.juxt.clojars-mirrors.ring/ring-core "1.9.2"]]

  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic]]}})
