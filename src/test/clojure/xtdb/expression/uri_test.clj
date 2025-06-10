(ns xtdb.expression.uri-test
  (:require [clojure.test :as t]
            [xtdb.expression-test :as et]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-cast
  (t/is (= #xt/uri "https://xtdb.com"
           (et/project1 '(cast "https://xtdb.com" :uri) {})))

  (t/is (= #xt/uri "https://user:pass@xtdb.com"
           (et/project1 '(cast "https://user:pass@xtdb.com" :uri) {})))

  (t/is (= #xt/uri "https://xtdb.com"
           (et/project1 '(cast "https://xtdb.com" :uri) {})))

  (t/is (= "https://user:pass@xtdb.com"
           (et/project1 '(cast #xt/uri "https://user:pass@xtdb.com" :utf8) {}))))

(t/deftest test-extract
  (t/is (= "https" (et/project1 '(uri-scheme #xt/uri "https://xtdb.com") {})))
  (t/is (nil? (et/project1 '(uri-scheme #xt/uri "//xtdb.com") {})))

  (t/is (= "user:pass" (et/project1 '(uri-user-info #xt/uri "https://user:pass@xtdb.com") {})))
  (t/is (nil? (et/project1 '(uri-user-info #xt/uri "//xtdb.com") {})))

  (t/is (= "xtdb.com" (et/project1 '(uri-host #xt/uri "https://user@xtdb.com") {})))
  (t/is (nil? (et/project1 '(uri-host #xt/uri "///tmp/foo") {})))

  (t/is (= 80 (et/project1 '(uri-port #xt/uri "http://xtdb.com:80") {})))
  (t/is (nil? (et/project1 '(uri-port #xt/uri "http://xtdb.com") {})))

  (t/is (= "/tmp/foo" (et/project1 '(uri-path #xt/uri "file:///tmp/foo") {})))
  (t/is (= "/foo" (et/project1 '(uri-path #xt/uri "//tmp/foo") {})))
  (t/is (= "" (et/project1 '(uri-path #xt/uri "http://xtdb.com") {})))

  (t/is (= "foo=bar" (et/project1 '(uri-query #xt/uri "http://xtdb.com?foo=bar") {})))
  (t/is (= "foo=bar&baz=qux" (et/project1 '(uri-query #xt/uri "http://xtdb.com?foo=bar&baz=qux") {})))
  (t/is (nil? (et/project1 '(uri-query #xt/uri "http://xtdb.com") {})))

  (t/is (nil? (et/project1 '(uri-fragment #xt/uri "https://xtdb.com") {})))
  (t/is (= "foo" (et/project1 '(uri-fragment #xt/uri "https://xtdb.com#foo") {}))))
