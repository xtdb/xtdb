(ns core2.cli-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.cli :as cli]
            [integrant.core :as ig]))

(def core2-cli-edn
  (io/resource "core2/cli-test.edn"))

(def core2-cli-json
  (io/resource "core2/cli-test.json"))

(defn with-file-override [files f]
  (with-redefs [io/file (some-fn files io/file)]
    (f)))

(defn with-resource-override [resources f]
  (let [og io/resource]
    (with-redefs [io/resource (fn [& args]
                                (apply (some-fn resources og) args))]
      (f))))

(defmethod ig/init-key ::foo [_ opts] opts)

(t/deftest test-config-merging
  (letfn [(->system [cli-args]
            (-> (::cli/node-opts (cli/parse-args cli-args))
                ig/prep
                ig/init
                (->> (into {}))))]
    (t/testing "uses CLI supplied EDN file"
      (t/is (= {::foo {:bar {}}}
               (->system ["-f" (str (io/as-file core2-cli-edn))]))))

    (t/testing "uses core2.edn if present"
      (with-file-override {"core2.edn" (io/as-file core2-cli-edn)}
        (fn []
          (t/is (= {::foo {:bar {}}}
                   (->system []))))))

    (t/testing "uses CLI supplied JSON file"
      (t/testing "uses CLI supplied EDN file"
        (t/is (= {::foo {:baz {}}}
                 (->system ["-f" (str (io/as-file core2-cli-json))])))))

    (t/testing "uses core2.json if present"
      (with-file-override {"core2.json" (io/as-file core2-cli-json)}
        (fn []
          (t/is (= {::foo {:baz {}}}
                   (->system []))))))

    (t/testing "looks for core2.edn on classpath, prefers to core2.json"
      (with-resource-override {"core2.json" (io/as-file core2-cli-json)
                               "core2.edn" (io/as-file core2-cli-edn)}
        (fn []
          (t/is (= {::foo {:bar {}}}
                   (->system []))))))

    (t/testing "does also look for core2.json on the classpath"
      (with-resource-override {"core2.json" (io/as-file core2-cli-json)}
        (fn []
          (t/is (= {::foo {:baz {}}}
                   (->system []))))))))
