(defproject com.xtdb/xtdb-http-server "<inherited>"
  :description "XTDB HTTP Server"

  :plugins [[lein-parent "0.3.8"]
            [yogthos/lein-sass "0.1.10"]]

  :parent-project {:path "../../project.clj"
                   :inherit [:version :repositories :deploy-repositories
                             :managed-dependencies
                             :pedantic? :global-vars
                             :license :url :pom-addition]}

  :scm {:dir "../.."}

  :dependencies [[org.clojure/clojure]
                 [org.clojure/data.csv]
                 [com.xtdb/xtdb-core]
                 [com.cognitect/transit-clj]

                 [com.nimbusds/nimbus-jose-jwt]

                 [pro.juxt.clojars-mirrors.hiccup/hiccup "2.0.0-alpha2"]
                 [pro.juxt.clojars-mirrors.xtdb/xtdb-http-server-deps "0.0.2"]
                 [pro.juxt.clojars-mirrors.camel-snake-kebab/camel-snake-kebab "0.4.2"]]

  :clean-targets ^{:protect false} ["target"]
  :profiles {:dev
             {:jvm-opts ["-Dlogback.configurationFile=../../resources/logback-test.xml"]
              :dependencies [[metosin/reitit-dev "0.5.18"]

                             [org.clojure/clojurescript "1.11.60"]
                             [ch.qos.logback/logback-classic "1.2.11"]
                             [cljsjs/codemirror "5.44.0-1"]
                             [com.bhauman/figwheel-main "0.2.18"
                              :exclusions [com.google.errorprone/error_prone_annotations]]
                             [com.bhauman/rebel-readline-cljs "0.1.4"]
                             [reagent "1.1.1"]
                             [re-frame "1.2.0"]
                             [metosin/reitit-frontend "0.5.18"]
                             [metosin/reitit-spec "0.5.18"]
                             [fork "1.2.5"]
                             [day8.re-frame/http-fx "0.2.4"]
                             [tick "0.4.32"]
                             [cljsjs/react-datetime "2.16.2-0"]]}

             :test {:dependencies [[pro.juxt.clojars-mirrors.clj-http/clj-http "3.12.2"]
                                   [com.xtdb/xtdb-test]]}}

  :aliases {"fig" ["trampoline" "run" "-m" "figwheel.main"]
            "build:cljs" ["do"
                          ["clean"]
                          ["run" "-m" "figwheel.main" "-O" "advanced" "-bo" "dev"]
                          ["sass"]]
            "install" ["do" "build:cljs" "install"]}

  :resource-paths ["resources" "cljs-target" "src-cljs"]
  :jar-exclusions [#"public/cljs-out/dev/.*"]
  :sass {:source "resources/public/scss/" :target "cljs-target/public/css/"})
