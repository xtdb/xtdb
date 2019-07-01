(defproject juxt/crux-dev :derived-from-git
  :description "Crux Dev Dependencies"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/test.check "0.10.0-alpha3"]
                 [org.eclipse.rdf4j/rdf4j-rio-ntriples "2.5.1"]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "2.5.1"]
                 [org.rocksdb/rocksdbjni "6.0.1"]]
  :middleware [leiningen.project-version/middleware])
