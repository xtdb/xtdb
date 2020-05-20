(defproject juxt/crux-http-server "crux-git-version-alpha"
  :description "Crux HTTP Server"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.csv "1.0.0"]
                 [hiccup "2.0.0-alpha2"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [metosin/reitit "0.4.2"]
                 [ring/ring-core "1.8.0"]
                 [ring/ring-jetty-adapter "1.8.0"]
                 [ring/ring-codec "1.1.2"]
                 [ring-cors "0.1.13"]
                 [metosin/muuntaja "0.6.6"]
                 [com.nimbusds/nimbus-jose-jwt "8.2.1" :exclusions [net.minidev/json-smart]]
                 [net.minidev/json-smart "2.3"]]
  :clean-targets ^{:protect false} ["target"]
  :profiles {:dev
             {:dependencies [[org.clojure/clojurescript "1.10.339"]
                             [ch.qos.logback/logback-classic "1.2.3"]
                             [cljsjs/codemirror "5.44.0-1"]
                             [com.bhauman/figwheel-main "0.2.4"]
                             [com.bhauman/rebel-readline-cljs "0.1.4"]
                             [reagent "0.10.0"]
                             [re-frame "0.12.0"]
                             [fork "1.2.5"]
                             [day8.re-frame/http-fx "v0.2.0"]
                             [bidi "2.1.6"]
                             [tick "0.4.23-alpha"]
                             [kibu/pushy "0.3.8"]]}
             :sass-from-root {:sass {:source "crux-http-server/resources/public/scss/"
                                     :target "crux-http-server/cljs-target/public/css/"}}}
  :aliases {"fig" ["trampoline" "run" "-m" "figwheel.main"]
            "build:cljs" ["do"
                          ["clean"]
                          ["run" "-m" "figwheel.main" "-O" "advanced" "-bo" "dev"]
                          ["sass"]]
            "install" ["do"
                       ["with-profiles" "+sass-from-root" "build:cljs"]
                       "install"]}
  :plugins [[yogthos/lein-sass "0.1.10"]]
  :resource-paths ["resources" "cljs-target" "src-cljs"]
  :sass {:source "resources/public/scss/" :target "cljs-target/public/css/"}
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
