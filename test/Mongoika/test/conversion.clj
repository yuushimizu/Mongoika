(ns mongoika.test.conversion
  (use mongoika.conversion
       clojure.test)
  (import [com.mongodb BasicDBObject]))

(deftest mongo-object<--test
  (is (= "foo" (mongo-object<- "foo")))
  (is (= "bar" (mongo-object<- :bar)))
  (is (= "baz" (mongo-object<- 'baz)))
  (is (= (doto (BasicDBObject.)
           (.put "name" "Jack")
           (.put "age" 30)
           (.put "points" 560))
         (mongo-object<- {:name "Jack" :age 30 :points 560})))
  (is (= ["foo" "bar" "baz"] (mongo-object<- [:foo 'bar "baz"])))
  (is (= (doto (BasicDBObject.)
           (.put "users" [(doto (BasicDBObject.)
                            (.put "name" "Jack")
                            (.put "age" 30))
                          (doto (BasicDBObject.)
                            (.put "name" "James")
                            (.put "age" 45))
                          (doto (BasicDBObject.)
                            (.put "name" "Joel")
                            (.put "age" 27))]))
         (mongo-object<- {:users [{:name "Jack" :age 30}
                                  {:name "James" :age 45}
                                  {:name "Joel" :age 27}]}))))

(deftest <-mongo-object-test
  (is (= "foo" (<-mongo-object "foo")))
  (is (= {:name "Jack" :age 30 :points 560}
         (<-mongo-object (doto (BasicDBObject.)
                           (.put "name" "Jack")
                           (.put "age" 30)
                           (.put "points" 560)))))
  (is (= ["foo" "bar" 12] (<-mongo-object ["foo" "bar" 12])))
  (is (= [{:name "Jack" :age 30}
          {:name "James" :age 45}
          {:name "Joel" :age 27}]
         (<-mongo-object [(doto (BasicDBObject.)
                            (.put "name" "Jack")
                            (.put "age" 30))
                          (doto (BasicDBObject.)
                            (.put "name" "James")
                            (.put "age" 45))
                          (doto (BasicDBObject.)
                            (.put "name" "Joel")
                            (.put "age" 27))]))))

;; (run-tests)