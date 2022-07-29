(ns core2.docker-test
  "Basic regression tests for our Dockerfile"
  (:require [clojure.test :refer [is deftest testing use-fixtures]]
            [clojure.java.shell :as sh]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [core2.test-util :as tu])
  (:import (java.util List)
           (java.util.concurrent TimeUnit)
           (java.net Socket)))

(set! *warn-on-reflection* false)

(def ^:dynamic *image* "An image sha for our Dockerfile, see (require-image)" nil)
(def ^:dynamic *container* "A running core2 container (Process), see (require-container)" nil)
(def ^:dynamic *pgwire-port* "The pg port of the running container" nil)
(def ^:dynamic *quiet* "Whether docker builds and so on should print" true)

;; label images so that can be cleaned up
(def img-label (str "core2dockertest"))
(defonce next-label (let [ctr (atom 0)] #(str img-label "=" (swap! ctr inc))))

(defn cmd-available? [cmd] (try (= 0 (:exit (sh/sh "command" "-v" cmd))) (catch Throwable _ false)))

(def docker-available? (partial cmd-available? "docker"))
(def psql-available? (partial cmd-available? "psql"))

(defn expected-working-dir? [] (.exists (io/file "Dockerfile")))

(defn docker
  "Returns a Process for docker with the given commands."
  [opts? & args]
  (let [opts (if (map? opts?) opts? {})
        args (if (map? opts?) args (cons opts? args))
        p (-> (ProcessBuilder. ^List (vec (cons "docker" (remove nil? args))))
              (cond-> (:inherit-io opts) (.inheritIO))
              (.start))]

    (when-some [[n tu] (:wait opts)]

      (when-not (.waitFor p n (or tu TimeUnit/MILLISECONDS))
        (.destroy p)
        (throw (Exception. "docker did not return in time")))

      (when-not (zero? (.exitValue p))
        (throw (Exception. "docker returned non-zero"))))

    p))

(defn prune-images []
  (let [prune (docker "image" "prune" "-f" "--filter" (str "label=" img-label))]
    (when-not (.waitFor prune 5 TimeUnit/SECONDS)
      (throw (Exception. "Took too long pruning images")))
    (when (not= 0 (.exitValue prune))
      (log/warn "Consider running at the terminal $" "docker" "image" "prune" "-f" "--filter" (str "label=" img-label))
      (throw (Exception. (format "Something went wrong pruning images (%s)" (.exitValue prune)))))))

(defn each-fixture [f]
  (binding [*image* nil
            *container* nil
            *pgwire-port* nil
            *quiet* *quiet*]
    (try
      (f)
      (finally
        (when *container* (.destroy *container*))))))

(defn once-fixture [f]
  (assert (expected-working-dir?) "working dir should be core2 root (with Dockerfile)")
  (assert (docker-available?) "docker must be on path to run docker tests")
  (try
    (f)
    (finally (prune-images))))

(use-fixtures :each #'each-fixture)
(use-fixtures :once #'once-fixture)

(defn require-image []
  (let [lbl (next-label)]
    (docker {:wait [5 TimeUnit/MINUTES]} "build" (when *quiet* "-q") "--label" lbl ".")
    (let [sha (str/trim (:out (sh/sh "docker" "images" "-q" "--filter" (str "label=" lbl))))]
      (when (thread-bound? #'*image*) (set! *image* sha))
      sha)))

(defn require-container []
  (let [sha (require-image)

        lines (atom [])

        port (tu/free-port)
        p (docker "run" (str "-p" port ":5432") "--rm" sha)

        consumer
        (delay
          (future
            (with-open [in (.getInputStream p), rdr (io/reader in)]
              (doseq [line (line-seq rdr)] (swap! lines conj line)))))

        outcome
        (delay
          (loop [wait-until (+ (System/currentTimeMillis) 20000)]
            (cond
              (<= wait-until (System/currentTimeMillis)) :timeout
              (not (.isAlive p)) :exited
              (some #(str/includes? % "PGWire server started on port") @lines) :started
              :else (do (Thread/sleep 10) (recur wait-until)))))]

    (try
      @consumer
      @outcome
      (when (not= :started @outcome)
        (throw (Exception. "Could not start container in time")))
      (catch Throwable e
        (when (realized? consumer) (future-cancel @consumer))
        (when (.destroy p))
        (throw e)))

    (when (thread-bound? #'*pgwire-port*) (set! *pgwire-port* port))
    (when (thread-bound? #'*container*) (set! *container* p))

    p))

(deftest build-test
  (testing "can build our Dockerfile without error"
    (let [build (docker "build" (when *quiet* "-q") "--label" img-label ".")
          stopped (.waitFor build 5 TimeUnit/MINUTES)]
      (is stopped)
      (is (= 0 (.exitValue build))))))

(deftest run-test
  (require-image)
  (testing "can run and close our Dockerfile"
    (let [p (docker "run" "--rm" *image*)
          lines (atom [])

          consumer
          (delay
            (future
              (with-open [in (.getInputStream p), rdr (io/reader in)]
                (doseq [line (line-seq rdr)] (swap! lines conj line)))))

          outcome
          (delay
            (loop [wait-until (+ (System/currentTimeMillis) 20000)]
              (cond
                (<= wait-until (System/currentTimeMillis)) :timeout
                (not (.isAlive p)) :exited
                (some #(str/includes? % "PGWire server started on port") @lines) :started
                :else (do (Thread/sleep 10) (recur wait-until)))))]
      (try
        @consumer
        (is (= :started @outcome))
        (.destroy p)
        (is (.waitFor p 1000 TimeUnit/MILLISECONDS))
        (when-not (.isAlive p)
          (is (= 143 (.exitValue p)) "SIGTERM exit"))
        (finally
          (future-cancel @consumer)
          (.destroy p))))))

(deftest run-and-connect-test
  (require-container)
  (testing "socket connect"
    (with-open [_ (Socket. "localhost" (int *pgwire-port*))]))
  (when (psql-available?)
    (testing "can run a query using psql"
      (is (= 0 (:exit (sh/sh "psql" "-hlocalhost" (str "-p" *pgwire-port*) "-c" "select ping")))))))

;; set var meta for all tests in this ns
(doseq [var (vals (ns-publics (.-name *ns*)))]
  (alter-meta! var assoc :docker true, :requires-docker true))
