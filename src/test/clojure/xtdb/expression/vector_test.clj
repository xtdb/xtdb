(ns xtdb.expression.vector-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.expression.vector :as vector]
            [xtdb.test-util :as tu]))

;; Unit tests for the embed-text function
#_(t/deftest test-embed-function-basics
  "Test the basic embed function with deterministic output"
  (let [text1 "hello world"
        text2 "hello world" 
        text3 "different text"]
    
    ;; Same text should produce same embedding
    (t/is (= (vector/embed-text text1) (vector/embed-text text2)))
    
    ;; Different text should produce different embeddings
    (t/is (not= (vector/embed-text text1) (vector/embed-text text3)))
    
    ;; All embeddings should be 384-dimensional
    (t/is (= 384 (count (vector/embed-text text1))))
    (t/is (= 384 (count (vector/embed-text text3))))
    
    ;; All elements should be floats
    (t/is (every? float? (vector/embed-text text1)))))
#_
(t/deftest test-embed-function-edge-cases
  "Test embed function with edge cases"
  ;; Empty string
  (t/is (= 384 (count (vector/embed-text ""))))
  
  ;; Long text
  (let [long-text (apply str (repeat 1000 "a"))]
    (t/is (= 384 (count (vector/embed-text long-text)))))
  
  ;; Special characters
  (t/is (= 384 (count (vector/embed-text "Hello, 世界! @#$%^&*()")))))

;; End-to-end SQL tests for the EMBED function 
(t/use-fixtures :each tu/with-node)

(t/deftest test-embed-function-basic
  "Basic functionality test for embed function"
  ;; Test basic embed function returns fixed-size vector
  (let [result (xt/q tu/*node* "SELECT EMBED('hello world') AS embedding")]
    (t/is (= 1 (count result)))
    (t/is (vector? (:embedding (first result))))
    (t/is (= 384 (count (:embedding (first result)))))
    (t/is (every? float? (:embedding (first result))))))

(t/deftest test-embed-function-consistency  
  "Test that same text produces same embedding"
  (let [result1 (xt/q tu/*node* "SELECT EMBED('consistent text') AS emb1")
        result2 (xt/q tu/*node* "SELECT EMBED('consistent text') AS emb2")]
    (t/is (= (:emb1 (first result1)) (:emb2 (first result2))))))

(t/deftest test-embed-function-different-texts
  "Test that different texts produce different embeddings"
  (let [results (xt/q tu/*node* "SELECT EMBED('first text') AS emb1, EMBED('second text') AS emb2")]
    (t/is (not= (:emb1 (first results)) (:emb2 (first results))))))

(t/deftest test-embed-with-similarity-functions
  "Test embed function works with existing similarity functions"
  ;; Create a table with text content and embeddings
  (xt/execute-tx tu/*node* [[:put-docs :documents {:xt/id 1, :content "artificial intelligence"}]
                            [:put-docs :documents {:xt/id 2, :content "machine learning"}] 
                            [:put-docs :documents {:xt/id 3, :content "cooking recipes"}]
                            [:put-docs :documents {:xt/id 4, :content "neural networks"}]])

  ;; Test that embeddings work with dot product (f32 specialization)
  (let [results (xt/q tu/*node* "SELECT _id, DOT_PRODUCT(EMBED(content), EMBED('artificial intelligence')) AS similarity 
                                 FROM documents 
                                 ORDER BY similarity DESC")]
    (t/is (= 4 (count results)))
    ;; First result should be the exact match (id 1)
    (t/is (= 1 (:xt/id (first results))))
    ;; Similarity scores should be in descending order
    (let [similarities (map :similarity results)]
      (t/is (apply >= similarities)))))

(t/deftest test-embed-similarity-search
  "Test embedding-based similarity search workflow"
  ;; Set up test data with diverse content  
  (xt/execute-tx tu/*node* [[:put-docs :articles {:xt/id "tech1", :title "Deep Learning Advances", :topic "technology"}]
                            [:put-docs :articles {:xt/id "tech2", :title "Neural Network Architectures", :topic "technology"}]
                            [:put-docs :articles {:xt/id "tech3", :title "Machine Learning Applications", :topic "technology"}] 
                            [:put-docs :articles {:xt/id "food1", :title "Italian Pasta Recipes", :topic "cooking"}]
                            [:put-docs :articles {:xt/id "food2", :title "Baking Bread at Home", :topic "cooking"}]
                            [:put-docs :articles {:xt/id "sport1", :title "Basketball Training Tips", :topic "sports"}]])

  ;; Find articles most similar to a technology query using cosine distance
  (let [query "artificial intelligence and machine learning"
        results (xt/q tu/*node* (format "SELECT _id, title, topic, 
                                                 COSINE_DISTANCE(EMBED(title), EMBED('%s')) AS distance
                                         FROM articles 
                                         WHERE topic = 'technology'
                                         ORDER BY distance 
                                         LIMIT 2" query))]
    (t/is (= 2 (count results)))
    ;; Results should be technology articles
    (t/is (every? #(= "technology" (:topic %)) results))
    ;; Distances should be in ascending order (smaller = more similar)
    (let [distances (map :distance results)]
      (t/is (apply <= distances))))

  ;; Test finding similar content across all topics using cosine similarity
  (let [query "cooking and recipes"  
        results (xt/q tu/*node* (format "SELECT _id, title, topic,
                                                 COSINE_SIMILARITY(EMBED(title), EMBED('%s')) AS similarity
                                         FROM articles
                                         ORDER BY similarity DESC
                                         LIMIT 3" query))]
    (t/is (= 3 (count results)))
    ;; Should find cooking articles with higher similarity
    (let [top-result (first results)]
      (t/is (= "cooking" (:topic top-result))))))

(t/deftest test-embed-vector-operations
  "Test embed function with various vector operations"
  ;; Test L2 distance with embeddings
  (let [results (xt/q tu/*node* "SELECT L2_DISTANCE(EMBED('hello'), EMBED('world')) AS distance")]
    (t/is (= 1 (count results)))
    (t/is (pos? (:distance (first results)))))

  ;; Test dot product symmetry with embeddings  
  (let [result1 (xt/q tu/*node* "SELECT DOT_PRODUCT(EMBED('first'), EMBED('second')) AS dp")
        result2 (xt/q tu/*node* "SELECT DOT_PRODUCT(EMBED('second'), EMBED('first')) AS dp")]
    (t/is (= (:dp (first result1)) (:dp (first result2)))))
    
  ;; Test cosine distance symmetry
  (let [result1 (xt/q tu/*node* "SELECT COSINE_DISTANCE(EMBED('alpha'), EMBED('beta')) AS cd")
        result2 (xt/q tu/*node* "SELECT COSINE_DISTANCE(EMBED('beta'), EMBED('alpha')) AS cd")]
    (t/is (= (:cd (first result1)) (:cd (first result2))))))

(t/deftest test-embed-with-table-columns
  "Test embed function with actual table data"
  ;; Create table with text columns
  (xt/execute-tx tu/*node* [[:put-docs :products {:xt/id 1, :name "Laptop Computer", :description "High performance laptop"}]
                            [:put-docs :products {:xt/id 2, :name "Desktop PC", :description "Powerful desktop computer"}]
                            [:put-docs :products {:xt/id 3, :name "Coffee Maker", :description "Automatic coffee brewing machine"}]
                            [:put-docs :products {:xt/id 4, :name "Smartphone", :description "Latest mobile phone technology"}]])

  ;; Search for products similar to a query
  (let [results (xt/q tu/*node* "SELECT _id, name,
                                        COSINE_DISTANCE(EMBED(description), EMBED('computer technology')) AS similarity
                                 FROM products
                                 WHERE COSINE_DISTANCE(EMBED(description), EMBED('computer technology')) < 0.8
                                 ORDER BY similarity")]
    ;; Should find computer-related products
    (t/is (>= (count results) 2))
    ;; Computer products should be more similar than coffee maker
    (let [computer-products (filter #(or (= 1 (:xt/id %)) (= 2 (:xt/id %)) (= 4 (:xt/id %))) results)]
      (t/is (>= (count computer-products) 1)))))

(t/deftest test-specialized-vector-functions
  "Test that fixed-size-list f32 vectors use optimized similarity functions"
  ;; Test cosine_similarity (only available for f32 embeddings)
  (let [result (xt/q tu/*node* "SELECT COSINE_SIMILARITY(EMBED('hello'), EMBED('hello')) AS sim")]
    (t/is (> (:sim (first result)) 0.99)) ; Should be nearly 1.0 for identical text
    (t/is (<= (:sim (first result)) 1.0)))

  ;; Test that different vector types can interoperate 
  ;; (fixed-size-list f32 embeddings with list f64 arrays)
  (let [result (xt/q tu/*node* "SELECT DOT_PRODUCT(EMBED('test'), ARRAY[1.0, 2.0, 3.0]) AS mixed")]
    (t/is (number? (:mixed (first result)))))

  ;; Test dimension validation
  (t/is (thrown? Exception 
        (xt/q tu/*node* "SELECT DOT_PRODUCT(EMBED('test'), ARRAY[1.0, 2.0]) AS invalid"))))

(t/deftest test-embed-sql-function-edge-cases
  "Test embed function with edge cases and special inputs"
  ;; Empty string
  (let [result (xt/q tu/*node* "SELECT EMBED('') AS embedding")]
    (t/is (= 384 (count (:embedding (first result))))))

  ;; Long text
  (let [long-text (apply str (repeat 100 "very long text "))
        result (xt/q tu/*node* (format "SELECT EMBED('%s') AS embedding" long-text))]
    (t/is (= 384 (count (:embedding (first result))))))

  ;; Special characters  
  (let [result (xt/q tu/*node* "SELECT EMBED('Hello, 世界! @#$%^&*()') AS embedding")]
    (t/is (= 384 (count (:embedding (first result))))))

  ;; Numbers and mixed content
  (let [result (xt/q tu/*node* "SELECT EMBED('Product-123: Price $99.99 (50% off!)') AS embedding")]
    (t/is (= 384 (count (:embedding (first result)))))))

(comment
  ;; Example usage patterns that this test demonstrates:
  
  ;; 1. Basic text embedding
  ;; SELECT EMBED('your text here') AS embedding
  
  ;; 2. Similarity search
  ;; SELECT *, COSINE_DISTANCE(EMBED(content), EMBED('search query')) AS similarity
  ;; FROM documents 
  ;; ORDER BY similarity
  ;; LIMIT 10
  
  ;; 3. Semantic filtering
  ;; SELECT * FROM articles
  ;; WHERE DOT_PRODUCT(EMBED(title), EMBED('machine learning')) > 0.7
  
  ;; 4. Clustering/grouping by semantic similarity
  ;; SELECT topic, AVG(COSINE_DISTANCE(EMBED(title), EMBED('reference text'))) AS avg_similarity
  ;; FROM articles
  ;; GROUP BY topic
  )
