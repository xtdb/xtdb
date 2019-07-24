(ns edge.main
  (:require
    edge.system
    [integrant.core :as ig]))

(def system nil)

(defn -main [& args]
  (let [system-config (edge.system/system-config {:profile :prod})
        sys (ig/init system-config)]
    (.addShutdownHook
      (Runtime/getRuntime)
      (Thread.
        (fn []
          (ig/halt! sys))))
    (alter-var-root #'system (constantly sys)))
  @(promise))
