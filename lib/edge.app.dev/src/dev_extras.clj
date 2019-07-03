;; Copyright © 2016-2019, JUXT LTD.
(ns ^{:clojure.tools.namespace.repl/load false} dev-extras
  (:require
   [clojure.test :refer [run-tests]]
   [edge.system :as system]
   [edge.system.meta :as system.meta]
   [integrant.repl]
   [integrant.repl.state]
   io.aviso.ansi
   clojure.tools.deps.alpha.repl)
  (:import
    [org.slf4j.bridge SLF4JBridgeHandler]))

(SLF4JBridgeHandler/removeHandlersForRootLogger)
(SLF4JBridgeHandler/install)

(when (try
        (require 'figwheel.main.logging)
        true
        (catch Throwable _))
  ;; Undo default logger being extremely fine grained in figwheel,
  ;; in order to configure figwheel to delegate to slf4j.
  (let [l @(resolve 'figwheel.main.logging/*logger*)]
    ((resolve 'figwheel.main.logging/remove-handlers) l)
    (.setUseParentHandlers l true)))

(defmacro ^:private proxy-ns
  [ns & vars]
  (cons `do
        (map (fn [v] `(do (def ~v ~(symbol (str ns) (str v)))
                          (alter-meta!
                            (resolve '~v)
                            merge
                            (select-keys (meta (resolve '~(symbol (str ns) (str v))))
                                         [:doc :file :line :column :arglists]))))
             vars)))
 
(proxy-ns integrant.repl clear halt prep init reset reset-all suspend)
(proxy-ns clojure.tools.deps.alpha.repl add-lib)

(defmacro ^:private watch-var
  [s alias]
  `(do
     (def ~alias ~s)
     (add-watch (var ~s)
                (keyword "dev-extras" ~(name alias))
                (fn [_# _# _# new#]
                  (alter-var-root
                    (var ~alias)
                    (constantly new#))))))

(watch-var integrant.repl.state/system system)
(watch-var integrant.repl.state/config system-config)

(defn go []
  (let [res (integrant.repl/go)]
    (doseq [message (system.meta/useful-infos system-config system)]
      (println (io.aviso.ansi/yellow (format "[Edge] %s" message))))
    (println (str (io.aviso.ansi/yellow "[Edge] Now make code changes, then enter ")
                  (io.aviso.ansi/bold-yellow "(reset)")
                  (io.aviso.ansi/yellow " here")))
    res))

(defn resume []
  (let [res (integrant.repl/resume)]
    (doseq [message (system.meta/useful-infos system-config system)]
      (println (io.aviso.ansi/yellow (format "[Edge] %s" message))))
    res))

(integrant.repl/set-prep! #(system/system-config {:profile :dev}))

(defn set-prep!
  [aero-opts]
  (integrant.repl/set-prep! #(system/system-config aero-opts)))

(defn- test-namespaces
  []
  (keep (fn [[ns vars]]
          (when (some (comp :test meta) vars) ns))
        (map (juxt identity (comp vals ns-publics))
             (all-ns))))

(defn test-all []
  (apply run-tests (test-namespaces)))

(defn reset-and-test []
  (reset)
  (time (test-all)))

(defn cljs-repl
  "Start a ClojureScript REPL"
  ([]
   ;; ensure system is started - this could be less effectful perhaps?
   (go)
   (if (try
         (require 'figwheel-sidecar.repl-api)
         (catch java.io.FileNotFoundException _
           false))
     (eval
       `(do
          (require 'figwheel-sidecar.repl-api)
          (figwheel-sidecar.repl-api/cljs-repl)))
     (eval
       `(do
          (require 'figwheel.main.api)
          (require 'figwheel.main)
          (require 'figwheel.repl)
          (let [builds# (keys @figwheel.main/build-registry)]
            (if (= (count builds#) 1)
              (binding [figwheel.repl/*server* true]
                (figwheel.main.api/cljs-repl (first builds#)))
              (throw (ex-info "A build must be specified, please call with an argument"
                              {:builds builds#}))))))))
  ([build-id]
   ;; Register build with figwheel
   (go)
   ;; Assume figwheel main
   (eval
     `(do
        (require 'figwheel.main.api)
        (require 'figwheel.repl)
        (binding [figwheel.repl/*server* true]
          (figwheel.main.api/cljs-repl ~build-id))))))
