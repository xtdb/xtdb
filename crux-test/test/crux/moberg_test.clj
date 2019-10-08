(ns crux.moberg-test
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tcct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [crux.codec :as c]
            [crux.fixtures :as f]
            [crux.fixtures.kv-only :as fkv :refer [*kv* *kv-module*]]
            [crux.kv :as kv]
            [crux.moberg :as moberg]
            [crux.status :as status])
  (:import crux.api.NonMonotonicTimeException))

(t/use-fixtures :each fkv/with-each-kv-store-implementation fkv/without-kv-index-version fkv/with-kv-store f/with-silent-test-check)

(t/deftest test-can-send-and-receive-message
  (t/is (= 1 (moberg/end-message-id-offset *kv* :my-topic)))
  (let [{:crux.moberg/keys [message-id message-time topic]
         :as submitted-message} (moberg/sent-message->edn (moberg/send-message *kv* :my-topic "Hello World"))]
    (t/is (integer? message-id))
    (t/is (inst? message-time))
    (t/is (= :my-topic topic))

    (with-open [snapshot (kv/new-snapshot *kv*)
                i (kv/new-iterator snapshot)]
      (t/is (= (merge submitted-message
                      {:crux.moberg/body "Hello World"}) (moberg/message->edn (moberg/seek-message i :my-topic))))
      (t/is (nil? (moberg/next-message i :my-topic))))

    (t/is (= (inc message-id) (moberg/end-message-id-offset *kv* :my-topic)))))

(t/deftest test-detects-backwards-clock-drift
  (let [{:crux.moberg/keys [message-id message-time]
         :as submitted-message} (moberg/sent-message->edn (moberg/send-message *kv* :my-topic "Hello World"))]
    (t/is (= (inc message-id) (moberg/end-message-id-offset *kv* :my-topic)))

    (with-redefs [crux.moberg/now (fn [] #inst "2019")]
      (t/is (thrown-with-msg?
             NonMonotonicTimeException
             (re-pattern (str "Clock has moved backwards in time, message id: "
                              1583412019200001
                              " was generated using " (pr-str #inst "2019")
                              " lowest valid next id: "
                              (inc message-id)
                              " was generated using " (pr-str message-time)))
             (moberg/sent-message->edn (moberg/send-message *kv* :my-topic "Hello World")))))))

(t/deftest test-can-send-and-receive-message-on-two-topics
  (let [my-topic-message (moberg/sent-message->edn (moberg/send-message *kv* :my-topic "Hello World"))
        your-topic-message (moberg/sent-message->edn (moberg/send-message *kv* :your-topic "Hello World"))]
    (with-open [snapshot (kv/new-snapshot *kv*)
                i (kv/new-iterator snapshot)]
      (t/is (= (merge my-topic-message
                      {:crux.moberg/body "Hello World"}) (moberg/message->edn (moberg/seek-message i :my-topic))))
      (t/is (nil? (moberg/next-message i :my-topic))))

    (with-open [snapshot (kv/new-snapshot *kv*)
                i (kv/new-iterator snapshot)]
      (t/is (= (merge your-topic-message
                      {:crux.moberg/body "Hello World"}) (moberg/message->edn (moberg/seek-message i :your-topic))))
      (t/is (nil? (moberg/next-message i :your-topic))))))

(t/deftest test-can-send-and-receive-multiple-messages
  (dotimes [n 10]
    (moberg/send-message *kv* :my-topic n))
  (with-open [snapshot (kv/new-snapshot *kv*)
              i (kv/new-iterator snapshot)]
    (t/is (= 0 (.body (moberg/seek-message i :my-topic))))
    (dotimes [n 9]
      (t/is (= (inc n) (.body (moberg/next-message i :my-topic)))))
    (t/is (nil? (moberg/next-message i :my-topic)))

    (t/is (= 0 (.body (moberg/seek-message i :my-topic))))))

(t/deftest test-can-send-and-receive-messages-with-compaction
  (let [compacted-message (moberg/sent-message->edn (moberg/send-message *kv* :my-topic :my-key "Hello World"))]
    (with-open [snapshot (kv/new-snapshot *kv*)
                i (kv/new-iterator snapshot)]
      (t/is (= (merge compacted-message
                      {:crux.moberg/topic :my-topic
                       :crux.moberg/key :my-key
                       :crux.moberg/body "Hello World"}) (moberg/message->edn (moberg/seek-message i :my-topic)))))

    (let [submitted-message (moberg/sent-message->edn (moberg/send-message *kv* :my-topic :my-key "Goodbye."))]
      (with-open [snapshot (kv/new-snapshot *kv*)
                  i (kv/new-iterator snapshot)]
        (t/is (= (merge submitted-message
                        {:crux.moberg/topic :my-topic
                         :crux.moberg/key :my-key
                         :crux.moberg/body "Goodbye."}) (moberg/message->edn (moberg/seek-message i :my-topic))))
        (t/is (nil? (moberg/next-message i :my-topic)))))))

(tcct/defspec test-basic-generative-send-and-seek-without-compaction 20
  (prop/for-all [[messages seek-to] (gen/let [topics (gen/not-empty (gen/vector gen/keyword 3))]
                                      (gen/tuple
                                       (gen/not-empty (gen/vector
                                                       (gen/tuple
                                                        (gen/elements topics)
                                                        (gen/not-empty gen/string-ascii))
                                                       10))
                                       (gen/one-of [(gen/return nil)
                                                    (gen/large-integer* {:min 0 :max 10})])))]
                (fkv/with-kv-store
                  (fn []
                    (let [submitted-messages (vec (for [[topic body] messages]
                                                    (merge (moberg/sent-message->edn (moberg/send-message *kv* topic body))
                                                           {:crux.moberg/topic topic
                                                            :crux.moberg/body body})))]
                      (with-open [snapshot (kv/new-snapshot *kv*)
                                  i (kv/new-iterator snapshot)]
                        (->> (for [[topic messages] (group-by :crux.moberg/topic submitted-messages)
                                   :let [messages (sort-by :crux.moberg/message-id messages)
                                         [messages seek-id] (cond
                                                              (nil? seek-to)
                                                              [messages nil]

                                                              (>= seek-to (count messages))
                                                              [nil (inc (:crux.moberg/message-id (last messages)))]

                                                              :else
                                                              (let [messages (drop seek-to messages)]
                                                                [messages (:crux.moberg/message-id (first messages))]))]]
                               (t/is (= messages
                                        (when-let [m (moberg/seek-message i topic seek-id)]
                                          (->> (repeatedly #(moberg/next-message i topic))
                                               (take-while identity)
                                               (cons m)
                                               (mapv moberg/message->edn))))))
                             (every? true?))))))))

(tcct/defspec test-basic-generative-send-and-seek-with-compaction 20
  (prop/for-all [messages (gen/let [topics (gen/not-empty (gen/vector gen/keyword 3))
                                    keys (gen/not-empty (gen/vector gen/keyword 8))]
                            (gen/not-empty (gen/vector
                                            (gen/tuple
                                             (gen/elements topics)
                                             (gen/elements keys)
                                             (gen/not-empty gen/string-ascii)))))]
                (fkv/with-kv-store
                  (fn []
                    (let [submitted-messages (vec (for [[topic key body] messages]
                                                    (merge (moberg/sent-message->edn (moberg/send-message *kv* topic key body))
                                                           {:crux.moberg/topic topic
                                                            :crux.moberg/key key
                                                            :crux.moberg/body body})))]
                      (with-open [snapshot (kv/new-snapshot *kv*)
                                  i (kv/new-iterator snapshot)]
                        (->> (for [[topic messages] (group-by :crux.moberg/topic submitted-messages)
                                   :let [messages (for [[k messages] (group-by :crux.moberg/key messages)]
                                                    (last (sort-by :crux.moberg/message-id messages)))]]
                               (t/is (= (sort-by :crux.moberg/message-id messages)
                                        (map moberg/message->edn
                                             (cons (moberg/seek-message i topic)
                                                   (->> (repeatedly #(moberg/next-message i topic))
                                                        (take-while identity)))))))
                             (every? true?))))))))

(t/deftest test-micro-bench
  (if (Boolean/parseBoolean (System/getenv "CRUX_MOBERG_PERFORMANCE"))
    (let [n 1000000]
      (println *kv-module*)
      (time
       (dotimes [n n]
         (moberg/send-message *kv* :my-topic (str "Hello World-" n))))

      (prn (status/status-map *kv*))

      (time
       (with-open [snapshot (kv/new-snapshot *kv*)
                   i (kv/new-iterator snapshot)]
         (t/is (= (str "Hello World-" 0)
                  (.body (moberg/seek-message i :my-topic))))
         (dotimes [n n]
           (moberg/next-message i :my-topic)))))
    (t/is true)))
