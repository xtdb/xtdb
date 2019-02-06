(ns crux.moberg-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.fixtures :as f]
            [crux.kv :as kv]
            [crux.moberg :as moberg]))

(t/use-fixtures :each f/with-each-kv-store-implementation f/without-kv-index-version f/with-kv-store)

(t/deftest test-can-send-and-receive-message
  (let [{:crux.moberg/keys [message-id message-time]
         :as submitted-message} (moberg/send-message f/*kv* :my-topic "Hello World")]
    (t/is (integer? message-id))
    (t/is (inst? message-time))

    (with-open [snapshot (kv/new-snapshot f/*kv*)
                i (kv/new-iterator snapshot)]
      (t/is (= (merge submitted-message
                      {:crux.moberg/topic :my-topic
                       :crux.moberg/body "Hello World"}) (moberg/seek-message i :my-topic)))
      (t/is (nil? (moberg/next-message i :my-topic))))

    (t/is (= (inc message-id) (moberg/end-message-id-offset f/*kv* :my-topic)))))

(t/deftest test-can-send-and-receive-message-on-two-topics
  (let [my-topic-message (moberg/send-message f/*kv* :my-topic "Hello World")
        your-topic-message (moberg/send-message f/*kv* :your-topic "Hello World")]
    (with-open [snapshot (kv/new-snapshot f/*kv*)
                i (kv/new-iterator snapshot)]
      (t/is (= (merge my-topic-message
                      {:crux.moberg/topic :my-topic
                       :crux.moberg/body "Hello World"}) (moberg/seek-message i :my-topic)))
      (t/is (nil? (moberg/next-message i :my-topic))))

    (with-open [snapshot (kv/new-snapshot f/*kv*)
                i (kv/new-iterator snapshot)]
      (t/is (= (merge your-topic-message
                      {:crux.moberg/topic :your-topic
                       :crux.moberg/body "Hello World"}) (moberg/seek-message i :your-topic)))
      (t/is (nil? (moberg/next-message i :your-topic))))))

(t/deftest test-can-send-and-receive-multiple-messages
  (dotimes [n 10]
    (moberg/send-message f/*kv* :my-topic n))
  (with-open [snapshot (kv/new-snapshot f/*kv*)
              i (kv/new-iterator snapshot)]
    (t/is (= 0 (:crux.moberg/body (moberg/seek-message i :my-topic))))
    (dotimes [n 9]
      (t/is (= (inc n) (:crux.moberg/body (moberg/next-message i :my-topic)))))
    (t/is (nil? (moberg/next-message i :my-topic)))

    (t/is (= 0 (:crux.moberg/body (moberg/seek-message i :my-topic))))))

(t/deftest test-can-send-and-receive-messages-with-compaction
  (let [compacted-message (moberg/send-message f/*kv* :my-topic :my-key "Hello World")]
    (with-open [snapshot (kv/new-snapshot f/*kv*)
                i (kv/new-iterator snapshot)]
      (t/is (= (merge compacted-message
                      {:crux.moberg/topic :my-topic
                       :crux.moberg/key :my-key
                       :crux.moberg/body "Hello World"}) (moberg/seek-message i :my-topic))))

    (let [submitted-message (moberg/send-message f/*kv* :my-topic :my-key "Goodbye.")]
      (with-open [snapshot (kv/new-snapshot f/*kv*)
                  i (kv/new-iterator snapshot)]
        (t/is (= (merge submitted-message
                        {:crux.moberg/topic :my-topic
                         :crux.moberg/key :my-key
                         :crux.moberg/body "Goodbye."}) (moberg/seek-message i :my-topic)))
        (t/is (nil? (moberg/next-message i :my-topic)))))))

#_(t/deftest test-micro-bench
    (let [n 1000000]
      (time
       (dotimes [n n]
         (moberg/send-message f/*kv* :my-topic (str "Hello World-" n))))

      (time
       (with-open [snapshot (kv/new-snapshot f/*kv*)
                   i (kv/new-iterator snapshot)]
         (t/is (= (str "Hello World-" 0)
                  (:crux.moberg/body (moberg/seek-message i :my-topic))))
         (dotimes [n n]
           (moberg/next-message i :my-topic))))))
