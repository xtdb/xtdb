(defproject juxt/crux-example-standalone-webservice "0.1.0-SNAPSHOT"
  :description "A example standalone webservice with crux"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [ring "1.7.0"] ; 1.7.1 breaks with Jetty ClassNotFound https://github.com/bhauman/figwheel-main/issues/95
;                 [ring/ring-core "1.7.1"]
;                 [ring/ring-jetty-adapter "1.7.1"]
                 [ring-cors "0.1.13"]

                 [juxt/crux-core "19.09-1.4.0-alpha"]
                 [juxt/crux-rocksdb "19.09-1.4.0-alpha"]
                 [juxt/crux-http-server "19.09-1.4.0-alpha"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [yada "1.3.0-alpha7"]
                 [hiccup "2.0.0-alpha2"]
                 [cljsjs/vega "4.4.0-0"]
                 [cljsjs/vega-lite "3.0.0-rc10-0"]
                 [cljsjs/vega-embed "3.26.0-0"]
                 [cljsjs/codemirror "5.44.0-1"]]
  :global-vars {*warn-on-reflection* true}
  :main example-standalone-webservice.main)
