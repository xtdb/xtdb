(defproject juxt/crux-http-server "crux-git-version-alpha"
  :description "Crux HTTP Server"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/data.csv "1.0.0"]
                 [hiccup "2.0.0-alpha2"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [metosin/reitit "0.5.12"]
                 [ring/ring-core "1.9.2"]
                 [info.sunng/ring-jetty9-adapter "0.14.2"]
                 [org.eclipse.jetty/jetty-alpn-openjdk8-server "9.4.36.v20210114"]
                 [ring/ring-codec "1.1.3"]
                 [ring-cors "0.1.13"]
                 [metosin/muuntaja "0.6.8"]
                 [com.cognitect/transit-clj "1.0.324"]

                 [com.nimbusds/nimbus-jose-jwt "9.7"]

                 [camel-snake-kebab "0.4.2"]]

  :clean-targets ^{:protect false} ["target"]
  :profiles {:dev
             {:jvm-opts ["-Dlogback.configurationFile=../resources/logback-test.xml"]
              :dependencies [[org.clojure/clojurescript "1.10.844"]
                             [ch.qos.logback/logback-classic "1.2.3"]
                             [cljsjs/codemirror "5.44.0-1"]
                             [com.bhauman/figwheel-main "0.2.12"
                              :exclusions [com.google.errorprone/error_prone_annotations]]
                             [com.bhauman/rebel-readline-cljs "0.1.4"]
                             [reagent "0.10.0"]
                             [re-frame "0.12.0"]
                             [fork "1.2.5"]
                             [day8.re-frame/http-fx "v0.2.0"]
                             [tick "0.4.23-alpha"]
                             [cljsjs/react-datetime "2.16.2-0"]

                             ;; dependency resolution
                             [com.bhauman/spell-spec "0.1.2"]
                             [ring/ring-devel "1.8.1"]
                             [expound "0.8.7"]]}

             :test {:dependencies [[clj-http "3.12.1"]
                                   [juxt/crux-test "crux-git-version"]]}}

  :aliases {"fig" ["trampoline" "run" "-m" "figwheel.main"]
            "build:cljs" ["do"
                          ["clean"]
                          ["run" "-m" "figwheel.main" "-O" "advanced" "-bo" "dev"]
                          ["sass"]]
            "install" ["do" "build:cljs" "install"]}
  :plugins [[yogthos/lein-sass "0.1.10"]]
  :resource-paths ["resources" "cljs-target" "src-cljs"]
  :jar-exclusions [#"public/cljs-out/dev/.*"]
  :sass {:source "resources/public/scss/" :target "cljs-target/public/css/"}
  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
