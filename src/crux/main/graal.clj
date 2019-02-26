(ns crux.main.graal
  (:require [crux.bootstrap.cli :as cli]
            [clojure.tools.logging :as log])
  (:gen-class))

(defn native-image? []
  (boolean (System/getProperty "org.graalvm.nativeimage.kind")))

(defn -main [& args]
  (log/info "Hello World!")
  #_(cli/start-system-from-command-line args))
