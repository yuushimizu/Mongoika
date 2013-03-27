(ns mongoika.test
  (use mongoika
       clojure.test
       [mongoika
        [conversion :only [mongo-object<- <-mongo-object]]])
  (import [clojure.lang IDeref IPending]
          [java.util Date Calendar]
          [java.io ByteArrayInputStream InputStream]
          [com.mongodb WriteConcern Mongo ServerAddress DB BasicDBObject DBCollection DBRef]
          [com.mongodb.gridfs GridFS]
          [org.bson.types ObjectId]))

(def test-server-1 {:host "127.0.0.1" :port 27017})

(defmacro ^{:private true} with-test-mongo [mongo-var & body]
  `(with-mongo [~mongo-var test-server-1 :safe true] ~@body))

(defn- clean-databases! [mongo]
  (doseq [db-name ["test-db-1" "test-db-2" "test-db-3"]]
    (.dropDatabase mongo db-name)))

(defmacro ^{:private true} with-test-db-binding [& body]
  `(with-test-mongo mongo#
     (clean-databases! mongo#)
     (with-db-binding (database mongo# :test-db-1)
       ~@body)))

(defn- ensure-collection-existing [collection-name]
  (let [collection (.getCollection (bound-db) (name collection-name))
        object (BasicDBObject.)]
    (.insert collection (into-array [object]))
    (remove collection object)))

(defmacro deftest* [name & body]
  `(deftest ~name
     (println)
     (println '~name)
     ~@body))

(deftest* mongo-options-test
  (let [options (mongo-options {:safe true :socketTimeout 100})]
    (is (= true (.safe options)))
    (is (= 100 (.socketTimeout options)))
    (is (= 0 (.wtimeout options))))
  (let [options (mongo-options {:w (.getW WriteConcern/NONE)})]
    (is (= WriteConcern/NONE (.getWriteConcern options)))
    (is (= 0 (.wtimeout options))))
  (let [options (mongo-options {})]
    (is (= false (.safe options)))
    (is (= 0 (.wtimeout options)))))

(deftest* server-address-test
  (is (ServerAddress. "localhost" 10000) (server-address {:host "localhost" :port 10000}))
  (is (ServerAddress. "192.168.0.1" 27000) (server-address {:host "192.168.0.1" :port 27000}))
  (is (ServerAddress. "localhost" 27017) (server-address {:host "localhost"}))
  (is (ServerAddress. "127.0.0.1" 10000) (server-address {:port 10000}))
  (is (ServerAddress. "127.0.0.1" 27017) (server-address {}))
  (is (ServerAddress. "localhost" 27017) (server-address "localhost"))
  (is (ServerAddress. "192.168.0.1" 27017) (server-address "192.168.0.1"))
  (is (ServerAddress. "localhost" 10000) (server-address (ServerAddress. "localhost" 10000)))
  (is (ServerAddress. "192.168.0.2" 20000) (server-address (ServerAddress. "192.168.0.2" 20000))))

(deftest* mongo-test
  (let [mongo (mongo)]
    (try
      (is (instance? Mongo mongo))
      (is (= [(ServerAddress. "127.0.0.1" 27017)] (vec (.getAllAddress mongo))))
      (finally (.close mongo))))
  (let [mongo (mongo test-server-1)]
    (try
      (is (instance? Mongo mongo))
      (is (= [(ServerAddress. (:host test-server-1) (:port test-server-1))] (vec (.getAllAddress mongo))))
      (finally (.close mongo))))
  (let [mongo (mongo [test-server-1])]
    (try
      (is (instance? Mongo mongo))
      (is (= [(ServerAddress. (:host test-server-1) (:port test-server-1))] (vec (.getAllAddress mongo))))
      (finally (.close mongo))))
  (let [mongo (mongo test-server-1 {:socketTimeout 10 :fsync true})]
    (try
      (is (instance? Mongo mongo))
      (is (= 10 (.socketTimeout (.getMongoOptions mongo))))
      (is (= true (.fsync (.getMongoOptions mongo))))
      (finally (.close mongo))))
  (let [mongo (mongo test-server-1 :safe true :socketTimeout 5)]
    (try
      (is (instance? Mongo mongo))
      (is (= WriteConcern/SAFE (.getWriteConcern mongo)))
      (is (= 5 (.socketTimeout (.getMongoOptions mongo))))
      (finally (.close mongo)))))

(deftest* with-mongo-test
  (with-mongo [mongo]
    (is (instance? Mongo mongo))
    (is (= [(ServerAddress. "127.0.0.1" 27017)] (vec (.getAllAddress mongo)))))
  (with-mongo [mongo test-server-1]
    (is (instance? Mongo mongo))
    (is (= [(ServerAddress. (:host test-server-1) (:port test-server-1))] (vec (.getAllAddress mongo)))))
  (with-mongo [mongo test-server-1]
    (is (instance? Mongo mongo))
    (is (= [(ServerAddress. (:host test-server-1) (:port test-server-1))] (vec (.getAllAddress mongo)))))
  (with-mongo [mongo test-server-1 {:socketTimeout 10 :fsync true}]
    (is (instance? Mongo mongo))
    (is (= 10 (.socketTimeout (.getMongoOptions mongo))))
    (is (= true (.fsync (.getMongoOptions mongo)))))
  (with-mongo [mongo test-server-1 :safe true :socketTimeout 5]
    (is (instance? Mongo mongo))
    (is (= WriteConcern/SAFE (.getWriteConcern mongo)))
    (is (= 5 (.socketTimeout (.getMongoOptions mongo)))))
  (with-mongo [mongo test-server-1 :safe true]
    (is (= WriteConcern/SAFE (.getWriteConcern mongo)))
    (with-mongo [mongo test-server-1 :safe false]
      (is (not (= WriteConcern/SAFE (.getWriteConcern mongo)))))
    (is (= WriteConcern/SAFE (.getWriteConcern mongo)))))

(deftest* database-test
  (with-test-mongo mongo
    (let [db (database mongo :test-db-1)]
      (is (instance? DB db))
      (is (= "test-db-1" (.getName db)))
      (is (= mongo (.getMongo db))))
    (let [db (database mongo "test-db-2")]
      (is (instance? DB db))
      (is (= "test-db-2" (.getName db)))
      (is (= mongo (.getMongo db)))))
  (with-mongo [mongo test-server-1 :safe true]
    (let [db (database mongo 'test-db-3)]
      (is (instance? DB db))
      (is (= "test-db-3" (.getName db)))
      (is (= mongo (.getMongo db)))
      (is (= WriteConcern/SAFE (.getWriteConcern db))))))

(deftest* set-default-db!-test
  (with-test-mongo mongo
    (clean-databases! mongo)
    (set-default-db! (database mongo :test-db-1))
    (is (instance? DB (bound-db)))
    (is (= "test-db-1" (.getName (bound-db))))
    (is (= mongo (.getMongo (bound-db))))
    (set-default-db! (database mongo :test-db-2))
    (is (instance? DB (bound-db)))
    (is (= "test-db-2" (.getName (bound-db))))
    (is (= mongo (.getMongo (bound-db))))))

(deftest* with-db-binding-test
  (with-test-mongo mongo
    (with-db-binding (database mongo :test-db-1)
      (is (instance? DB (bound-db)))
      (is (= "test-db-1" (.getName (bound-db))))
      (is (= mongo (.getMongo (bound-db)))))
    (with-db-binding (database mongo :test-db-2)
      (is (instance? DB (bound-db)))
      (is (= "test-db-2" (.getName (bound-db))))
      (is (= mongo (.getMongo (bound-db)))))))

(deftest* add-user-and-authenticate-test
  (with-test-mongo mongo
    (clean-databases! mongo)
    (with-db-binding (database mongo :test-db-1)
      (is (add-user! "tester42" "pswd4242"))
      (is (not (authenticated?)))
      (is (authenticate "tester42" "pswd4242"))
      (is (authenticated?)))
    (with-db-binding (database mongo :test-db-2)
      (is (add-user! "test-user-48" "password-of-test-user-48"))
      (is (not (authenticated?)))
      (is (authenticate "test-user-48" "password-of-test-user-48"))
      (is (authenticated?)))))

(deftest* collection-names-test
  (with-test-mongo mongo
    (clean-databases! mongo)
    (with-db-binding (database mongo :test-db-1)
      (is (empty? (collection-names)))
      (ensure-collection-existing :users)
      (is (= #{"system.indexes" "users"} (collection-names)))
      (ensure-collection-existing :comments)
      (is (= #{"system.indexes" "users" "comments"} (collection-names)))
      (ensure-collection-existing :items)
      (is (= #{"system.indexes" "users" "comments" "items"} (collection-names)))
      (.drop (.getCollection (bound-db) "users"))
      (is (= #{"system.indexes" "comments" "items"} (collection-names))))
    (with-db-binding (database mongo :test-db-2)
      (is (empty? (collection-names)))
      (ensure-collection-existing :test)
      (is (= #{"system.indexes" "test"} (collection-names))))
    (with-db-binding (database mongo :test-db-1)
      (is (= #{"system.indexes" "comments" "items"} (collection-names))))))

(deftest* collection-exists?-test
  (with-test-mongo mongo
    (clean-databases! mongo)
    (with-db-binding (database mongo :test-db-1)
      (is (not (collection-exists? :users)))
      (ensure-collection-existing :users)
      (is (collection-exists? :users))
      (is (not (collection-exists? :items)))
      (ensure-collection-existing :items)
      (is (collection-exists? :items)))
    (with-db-binding (database mongo :test-db-1)
      (is (collection-exists? :users))
      (is (collection-exists? :items)))))

(deftest* db-collection-test
  (with-test-db-binding
    (let [collection (db-collection :users)]
      (is (instance? DBCollection collection))
      (is (= "users" (.getName collection)))
      (is (= (bound-db) (.getDB collection))))
    (let [collection (db-collection "items")]
      (is (instance? DBCollection collection))
      (is (= "items" (.getName collection)))
      (is (= (bound-db) (.getDB collection))))
    (let [collection (db-collection 'comments)]
      (is (instance? DBCollection collection))
      (is (= "comments" (.getName collection)))
      (is (= (bound-db) (.getDB collection))))))

(deftest* collection-stats-test
  (with-test-db-binding
    (insert! :items {:name "apple" :price 120})
    (insert! :items {:name "orange" :price 90})
    (is (= 2 (:count (collection-stats :items))))
    (insert! :users {:name "Jack" :age 30})
    (is (= 1 (:count (collection-stats :users))))))

(deftest* simple-insert-and-query-test
  (with-test-db-binding
    (is (empty? (query :users)))
    (let [inserted (insert! :users {:name "Jimmy" :age 23 :badges {:gold 12 :silver 68 :bronze 112}})
          id (:_id inserted)]
      (is (instance? ObjectId id))
      (is (= {:_id id :name "Jimmy" :age 23 :badges {:gold 12 :silver 68 :bronze 112}} inserted))
      (is (= [inserted] (restrict :_id id (query :users)))))))

(deftest* query-source?-test
  (with-test-db-binding
    (is (query-source? :users))
    (is (query-source? "items"))
    (is (query-source? 'posts))
    (is (query-source? (db-collection :users)))
    (is (query-source? (grid-fs :images)))
    (is (query-source? (query :users)))
    (is (query-source? (restrict :age {>= 20} :users)))
    (is (not (query-source? [{:name "Apple" :price 100} {:name "Banana" :price 120}])))
    (is (not (query-source? [])))
    (is (not (query-source? nil)))
    (is (not (query-source? (list {:name "Apple" :price 100} {:name "Banana" :price 120}))))
    (is (not (query-source? (map :name (query :users)))))))

(deftest* ensure-index-test
  (with-test-db-binding
    (ensure-index! :items {:name :asc} :unique true)
    (ensure-index! :items {:price :desc})
    (ensure-index! :foods [:price :asc :name :asc])
    (ensure-index! :users [:group :asc :name :desc] :unique true)))

(deftest* query-test
  (with-test-db-binding
    (is (empty? (query :uers)))
    (let [banana (insert! :items {:name "Banana" :type "Fruit" :price 120})
          mikan (insert! :items {:name "Mikan" :type "Fruit" :price 60})
          apple (insert! :items {:name "Apple" :type "Fruit" :price 80})
          cola (insert! :items {:name "Cola" :type "Drink" :price 110})
          beer (insert! :items {:name "Beer" :type "Drink" :price 200})
          jack (insert! :users {:name "Jack" :age 30 :points 360 :items [apple mikan beer] :badges {:gold 30 :silver 120}})
          joel (insert! :users {:name "Joel" :age 23 :points 1700 :items [] :badges {:gold 231 :silver 12}})
          james (insert! :users {:name "James" :age 27 :points 60 :items [beer beer beer cola mikan mikan] :badges {:gold 13 :silver 189}})
          jimmy (insert! :users {:name "Jimmy" :age 22 :points 0 :items [banana banana banana] :badges {:gold 0 :silver 0}})]
      (testing "Simple restriction"
        (is (= [banana]
               (restrict :_id (:_id banana) (query :items))))
        (is (= [jack]
               (restrict :_id (:_id jack) (query :users))))
        (is (empty? (restrict :price {:$gt 300} (query :items)))))
      (testing "Order"
        (is (= [mikan apple banana]
               (order :price 1 (restrict :type "Fruit" (query :items)))))
        (is (= [banana apple mikan]
               (order :price -1 (restrict :type "Fruit" (query :items)))))
        (is (= [jimmy joel james jack]
               (order :age :asc (query :users))))
        (is (= [jack james joel jimmy]
               (order :age :desc (query :users))))
        (is (= [cola beer mikan apple banana]
               (order :type :asc :price :asc (query :items))))
        (is (= [beer cola banana apple mikan]
               (order :type :asc :price :desc (query :items))))
        (is (= [cola banana beer]
               (order :price :asc (restrict :price {:$gt 100} (query :items)))))
        (is (= [banana]
               (order :price :asc (restrict :price {:$gt 100} :type "Fruit" (query :items)))))
        (is (= [banana]
               (order :price :asc (restrict :price {:$gt 100} (restrict :type "Fruit" (query :items))))))
        (is (= [mikan apple banana]
               (order :price :asc (order :price :desc (restrict :type "Fruit" :items))))))
      (testing "Reverse order"
        (is (= [banana apple mikan]
               (reverse-order (order :price 1 (restrict :type "Fruit" (query :items))))))
        (is (= [apple mikan banana]
               (reverse-order (restrict :type "Fruit" (query :items)))))
        (is (= [banana apple mikan beer cola]
               (reverse-order (order :type :asc :price :asc (query :items)))))
        (is (= [mikan apple banana cola beer]
               (reverse-order (order :type :asc :price :desc (query :items))))))
      (testing "Projection"
        (is (= [{:name "Cola" :_id (:_id cola)}
                {:name "Banana" :_id (:_id banana)}
                {:name "Beer" :_id (:_id beer)}]
               (order :price :asc (project :name (restrict :price {:$gt 100} (query :items))))))
        (is (= [{:name "Cola" :price 110 :_id (:_id cola)}
                {:name "Banana" :price 120 :_id (:_id banana)}
                {:name "Beer" :price 200 :_id (:_id beer)}]
               (order :price :asc (project :name :price (restrict :price {:$gt 100} (query :items))))))
        (is (= [{:name "Cola" :_id (:_id cola)}
                {:name "Banana" :_id (:_id banana)}
                {:name "Beer" :_id (:_id beer)}]
               (order :price :asc (project :name (project :name :price (restrict :price {:$gt 100} (query :items)))))))
        (is (= [{:name "Cola" :_id (:_id cola)}
                {:name "Banana" :_id (:_id banana)}
                {:name "Beer" :_id (:_id beer)}]
               (order :price :asc (project :name :project (project :name (restrict :price {:$gt 100} (query :items)))))))
        (is (= [{:name "Cola" :price 110 :_id (:_id cola)}
                {:name "Banana" :price 120 :_id (:_id banana)}
                {:name "Beer" :price 200 :_id (:_id beer)}]
               (order :price :asc (project {:name true :price true} (restrict :price {:$gt 100} :items)))))
        (is (= [{:_id (:_id cola)} {:_id (:_id banana)} {:_id (:_id beer)}]
               (project :_id (order :price :asc (restrict :price {:$gt 100} :items)))))
        (is (= [{:_id (:_id cola)} {:_id (:_id banana)} {:_id (:_id beer)}]
               (project :type (project :name (order :price :asc (restrict :price {:$gt 100} :items))))))
        (is (= [{:name "Cola" :price 110 :_id (:_id cola)}
                {:name "Banana" :price 120 :_id (:_id banana)}
                {:name "Beer" :price 200 :_id (:_id beer)}]
               (order :price :asc (project {:name true} {:price true} (restrict :price {:$gt 100} :items)))))
        (is (= [{:name "Cola" :price 110 :_id (:_id cola)}
                {:name "Banana" :price 120 :_id (:_id banana)}
                {:name "Beer" :price 200 :_id (:_id beer)}]
               (order :price :asc (project :name {:price true} (restrict :price {:$gt 100} :items)))))
        (is (= [{:name "Cola" :price 110 :_id (:_id cola)}
                {:name "Banana" :price 120 :_id (:_id banana)}
                {:name "Beer" :price 200 :_id (:_id beer)}]
               (order :price :asc (project :name {:price true} (restrict :price {:$gt 100} :items)))))
        (is (= [{:name "Cola" :type "Drink" :_id (:_id cola)}
                {:name "Banana" :type "Fruit" :_id (:_id banana)}
                {:name "Beer" :type "Drink" :_id (:_id beer)}]
               (order :price :asc (project {:price false} (restrict :price {:$gt 100} :items)))))
        (is (= [{:name "Cola" :_id (:_id cola)}
                {:name "Banana" :_id (:_id banana)}
                {:name "Beer" :_id (:_id beer)}]
               (order :price :asc (project {:price false :type false} (restrict :price {:$gt 100} :items)))))
        (is (= [{:name "Cola" :_id (:_id cola)}
                {:name "Banana" :_id (:_id banana)}
                {:name "Beer" :_id (:_id beer)}]
               (order :price :asc (project {:price false} (project {:type false} (restrict :price {:$gt 100} :items))))))
        (is (= [{:name "Cola" :_id (:_id cola)}
                {:name "Banana" :_id (:_id banana)}
                {:name "Beer" :_id (:_id beer)}]
               (order :price :asc (project {:price false} (project :name :price (restrict :price {:$gt 100} :items))))))
        (is (= [{:_id (:_id cola)} {:_id (:_id banana)} {:_id (:_id beer)}]
               (order :price :asc (project {:name false :price false :type false} (restrict :price {:$gt 100} :items))))))
      (testing "Limit"
        (is (= [jack james]
               (limit 2 (order :age :desc (query :users)))))
        (is (= [cola]
               (limit 1 (order :price :desc (restrict :price {:$lt 115} (query :items))))))
        (is (= []
               (limit 0 :items)))
        (is (= [{:name "Cola"} {:name "Banana"}]
               (limit 2 [{:name "Cola"} {:name "Banana"} {:name "Beer"} {:name "Mikan"}])))
        (is (= [{:name "Apple"}]
               (limit 1 [{:name "Apple"} {:name "Banana"}])))
        (is (= [{:name "Cola"} {:name "Banana"}]
               (limit 2 [{:name "Cola"} {:name "Banana"}])))
        (is (= []
               (limit 0 [{:name "Cola"} {:name "Banana"}])))
        (is (= []
               (limit 10 [])))
        (is (= []
               (limit 2 nil))))
      (testing "Skip"
        (is (= [banana apple mikan]
               (skip 2 (order :type :asc :price :desc (query :items)))))
        (is (= [banana apple mikan]
               (skip 1 (skip 1 (order :type :asc (order :price :desc (query :items)))))))
        (is (= [beer cola banana apple mikan]
               (skip 0 (order :type :asc :price :desc (query :items)))))
        (is (= [{:name "Beer"} {:name "Mikan"} {:name "Apple"}]
               (skip 2 [{:name "Cola"} {:name "Banana"} {:name "Beer"} {:name "Mikan"} {:name "Apple"}])))
        (is (= [{:name "Banana"}]
               (skip 1 [{:name "Cola"} {:name "Banana"}])))
        (is (= []
               (skip 2 [{:name "Cola"} {:name "Banana"}])))
        (is (= []
               (skip 3 [])))
        (is (= []
               (skip 5 nil))))
      (testing "Limit and skip"
        (is (= [cola banana apple]
               (limit 3 (skip 1 (order :type :asc :price :desc (query :items))))))
        (is (= [cola banana apple]
               (limit 3 (limit 10 (skip 1 (order :type :asc :price :desc (query :items)))))))
        (is (= [cola banana]
               (limit 2 (skip 1 (order :price :asc (restrict :price {:$gt 70} (query :items)))))))
        (is (= [{:name "Mikan"} {:name "Apple"}]
               (limit 2 (skip 1 [{:name "Beer"} {:name "Mikan"} {:name "Apple"} {:name "Banana"} {:name "Cola"}])))))
      (testing "Make from DBCollection"
        (is (= [banana]
               (restrict :_id (:_id banana) (db-collection :items))))
        (is (= [jack]
               (restrict :_id (:_id jack) (db-collection :users))))
        (is (= [jimmy joel james jack]
               (order :age :asc (db-collection :users))))
        (is (= [{:name "Jack" :_id (:_id jack)}
                {:name "James" :_id (:_id james)}
                {:name "Joel" :_id (:_id joel)}
                {:name "Jimmy" :_id (:_id jimmy)}]
               (order :age :desc (project :name (db-collection :users)))))
        (is (= [mikan apple]
               (order :price :asc (limit 2 (db-collection :items)))))
        (is (= [cola banana beer]
               (order :price :asc (skip 2 (db-collection :items))))))
      (testing "Make from keyword"
        (is (= [banana]
               (restrict :_id (:_id banana) :items)))
        (is (= [jack]
               (restrict :_id (:_id jack) :users)))
        (is (= [jimmy joel james jack]
               (order :age :asc :users)))
        (is (= [{:name "Jack" :_id (:_id jack)}
                {:name "James" :_id (:_id james)}
                {:name "Joel" :_id (:_id joel)}
                {:name "Jimmy" :_id (:_id jimmy)}]
               (order :age :desc (project :name :users))))
        (is (= [mikan apple]
               (order :price :asc (limit 2 :items))))
        (is (= [cola banana beer]
               (order :price :asc (skip 2 :items)))))
      (testing "Lazy fetching"
        (let [all-items (query :items)
              sorted-items (order :price :asc all-items)
              all-fruits (restrict :type "Fruit" sorted-items)]
          (is (not (realized? all-items)))
          (is (not (realized? sorted-items)))
          (is (not (realized? all-fruits)))
          (is (= [mikan apple banana] all-fruits))
          (is (not (realized? all-items)))
          (is (not (realized? sorted-items)))
          (is (realized? all-fruits))
          (let [inexpensive-fruits (restrict :price {:$lt 100} all-fruits)]
            (is (not (realized? inexpensive-fruits)))
            (is (= [mikan apple] inexpensive-fruits))
            (is (realized? inexpensive-fruits)))
          (is (not (realized? all-items)))
          (is (not (realized? sorted-items)))
          (is (realized? all-fruits))
          (is (= [mikan apple cola banana beer] sorted-items))
          (is (not (realized? all-items)))
          (is (realized? sorted-items))
          (is (realized? all-fruits)))))))

(deftest* query-options-test
  (with-test-db-binding
    (let [banana (insert! :items {:name "Banana" :type "Fruit" :price 120})
          mikan (insert! :items {:name "Mikan" :type "Fruit" :price 60})
          apple (insert! :items {:name "Apple" :type "Fruit" :price 80})
          cola (insert! :items {:name "Cola" :type "Drink" :price 110})
          beer (insert! :items {:name "Beer" :type "Drink" :price 200})]
      (is (= [mikan apple cola banana beer] (order :price :asc (query-options :notimeout :items))))
      (is (= [mikan apple] (order :price :asc (query-options :notimeout (limit 2 :items)))))
      (is (= [beer banana cola] (order :price :desc (query-options com.mongodb.Bytes/QUERYOPTION_NOTIMEOUT (limit 3 :items))))))))

(deftest* postapply-test
  (with-test-db-binding
    (let [banana (insert! :items {:name "Banana" :type "Fruit" :price 120 :number 1})
          mikan (insert! :items {:name "Mikan" :type "Fruit" :price 60 :number 2})
          apple (insert! :items {:name "Apple" :type "Fruit" :price 80 :number 3})
          cola (insert! :items {:name "Cola" :type "Drink" :price 110 :number 3})
          beer (insert! :items {:name "Beer" :type "Drink" :price 200 :number 2})]
      (is (= [[mikan apple] [cola banana] [beer]] (postapply #(partition-all 2 %) (order :price :items))))
      (is (= [apple banana cola] (postapply #(filter (fn [item] (odd? (:number item))) %) (order :name :items))))
      (is (= [apple banana cola] (order :name :items (postapply #(filter (fn [item] (odd? (:number item))) %) :items))))
      (is (nil? (fetch-one (order :price (postapply #(filter (fn [item] (= 3 (:number item))) %) :items))))))))

(deftest* map-after-test
  (with-test-db-binding
    (let [users (map-after (fn [user]
                             (assoc user
                               :reversed-name (apply str (reverse (:name user)))))
                           :users)
          jack (insert! users {:name "Jack" :age 20})
          jimmy (insert! users {:name "Jimmy" :age 23})
          [james joel] (insert-multi! users {:name "James" :age 17} {:name "Joel" :age 30})]
      (is (= {:_id (:_id jack) :name "Jack" :reversed-name "kcaJ" :age 20} jack))
      (is (= {:_id (:_id jimmy) :name "Jimmy" :reversed-name "ymmiJ" :age 23} jimmy))
      (is (= {:_id (:_id james) :name "James" :reversed-name "semaJ" :age 17} james))
      (is (= {:_id (:_id joel) :name "Joel" :reversed-name "leoJ" :age 30} joel))
      (is (= {:_id (:_id jack) :name "Jack" :reversed-name "kcaJ" :age 20}
             (fetch-one (restrict :_id (:_id jack) users))))
      (is (= [{:_id (:_id james) :name "James" :reversed-name "semaJ" :age 17}
              {:_id (:_id jack) :name "Jack" :reversed-name "kcaJ" :age 20}
              {:_id (:_id jimmy) :name "Jimmy" :reversed-name "ymmiJ" :age 23}
              {:_id (:_id joel) :name "Joel" :reversed-name "leoJ" :age 30}]
             (order :age :asc users)))
      (is (= {:_id (:_id jack) :name "Jack" :age 20}
             (fetch-one (restrict :_id (:_id jack) :users))))
      (is (= [{:_id (:_id james) :name "James" :age 17}
              {:_id (:_id jack) :name "Jack" :age 20}
              {:_id (:_id jimmy) :name "Jimmy" :age 23}
              {:_id (:_id joel) :name "Joel" :age 30}]
             (order :age :asc :users)))
      (let [new-jack (update! :$set {:age 21} (restrict :_id (:_id jack) users))]
        (is (= {:_id (:_id jack) :name "Jack" :reversed-name "kcaJ" :age 21} new-jack))
        (is (= {:_id (:_id jack) :name "Jack" :reversed-name "kcaJ" :age 21}
               (fetch-one (restrict :_id (:_id jack) users))))
        (is (= {:_id (:_id jack) :name "Jack" :age 21}
               (fetch-one (restrict :_id (:_id jack) :users)))))
      (is (= ["James" "Jack" "Jimmy" "Joel"]
             (order :age :asc (map-after :name :users))))
      (is (= ["semaJ" "kcaJ" "ymmiJ" "leoJ"]
             (order :age :asc (map-after :reversed-name users))))
      (is (= [60 46 42 34]
             (map-after #(* 2 %) (order :age :desc (map-after :age :users)))))
      (is (nil? (fetch-one (map-after #(assoc % :rank 1) (restrict :_id 0 :users)))))
      (is (nil? (update! :$set {:age 10} (map-after #(assoc % :rank 1) (restrict :_id 0 :users)))))
      (is (empty? (map-after #(assoc % :rank 1) (restrict :_id 0 :users)))))))

(deftest* restriction-special-keys-test
  (with-test-db-binding
    (let [banana (insert! :items {:name "Banana" :type "Fruit" :price 120 :param 10})
          mikan (insert! :items {:name "Mikan" :type "Fruit" :price 60 :param "Orange"})
          apple (insert! :items {:name "Apple" :type "Fruit" :price 80 :param 60})
          cola (insert! :items {:name "Cola" :type "Drink" :price 110 :param ""})
          beer (insert! :items {:name "Beer" :type "Drink" :price 200 :param 10})]
      (is (= [mikan apple]
             (order :price :asc (restrict :price {< 110} :items))))
      (is (= [mikan apple cola]
             (order :price :asc (restrict :price {<= 110} :items))))
      (is (= [banana beer]
             (order :price :asc (restrict :price {> 110} :items))))
      (is (= [cola banana beer]
             (order :price :asc (restrict :price {>= 110} :items))))
      (is (= [mikan apple]
             (order :price :asc (restrict :price {not {>= 110}} :items))))
      (is (= [mikan banana]
             (order :price :asc (restrict :price {mod [30 0]} :items))))
      (is (= [apple cola beer]
             (order :price :asc (restrict :price {mod [30 20]} :items))))
      (is (= [mikan cola]
             (order :price :asc (restrict :param {type 2} :items))))
      (is (= [apple banana beer]
             (order :price :asc (restrict :param {type 18} :items))))
      (is (= [mikan apple cola beer]
             (order :price :asc (restrict :$or [{:type "Drink"}
                                                {:price {< 100}}]
                                          :items)))))))

(deftest* fetch-one-test
  (with-test-db-binding
    (let [cat (insert! :animals {:name "Cat" :legs 4 :arms 0 :tails 1})]
      (testing "Fetch with Keyword"
        (is (= cat (fetch-one :animals))))
      (testing "Fetch with DBCollection"
        (is (= cat (fetch-one (db-collection :animals)))))
      (testing "Fetch with query"
        (is (= cat (fetch-one (query :animals))))
        (let [dog (insert! :animals {:name "Dog" :legs 4 :arms 0 :tails 1})
              snake (insert! :animals {:name "Snake" :legs 0 :arms 0 :tails 1})
              human (insert! :animals {:name "Human" :legs 2 :arms 2 :tails 0})
              octopus (insert! :animals {:name "Octopus" :legs 0 :arms 8 :tails 0})]
          (is (= cat (fetch-one (restrict :_id (:_id cat) :animals))))
          (is (= human (fetch-one (restrict :name "Human" :animals))))
          (is (= snake (fetch-one (restrict :legs 0 :tails 1 :animals))))
          (is (= {:_id (:_id dog) :name "Dog" :tails 1}
                 (fetch-one (project :name :tails (restrict :name "Dog" :animals)))))
          (is (= {:_id (:_id dog) :name "Dog"}
                 (fetch-one (project :name (restrict :name "Dog" :animals)))))
          (testing "Fetch with order"
            (is (= octopus (fetch-one (order :arms :desc :animals))))
            (is (= snake (fetch-one (order :name :desc :animals))))
            (is (= human (fetch-one (order :arms :asc (restrict :tails {:$lt 1} :animals)))))
            (is (= {:_id (:_id human) :name "Human"}
                   (fetch-one (order :arms :asc (project :name (restrict :tails {:$lt 1} :animals)))))))
          (testing "Fetch with skip"
            (is (= dog (fetch-one (skip 1 (order :name :asc :animals)))))
            (is (= human (fetch-one (skip 3 (order :tails :desc :arms :asc :animals)))))
            (is (= snake (fetch-one (skip 1 (order :legs :asc :tails :asc (restrict :legs {:$lt 4} :animals))))))
            (is (= {:_id (:_id snake) :name "Snake"}
                   (fetch-one (skip 1 (order :legs :asc :tails :asc (project :name (restrict :legs {:$lt 4} :animals)))))))))))))

(deftest* count-test
  (with-test-db-binding
    (is (= 0 (count (query :users))))
    (insert! :users {:name "John" :age 20})
    (is (= 1 (count (query :users))))
    (insert-multi! :users
                   {:name "James" :age 17}
                   {:name "Joel" :age 33}
                   {:name "Jack" :age 26}
                   {:name "Johan" :age 19}
                   {:name "Jane" :age 22})
    (is (= 6 (count (query :users))))
    (is (= 2 (count (restrict :age {:$lt 20} :users))))
    (is (= 1 (count (restrict :age {:$gt 30} :users))))
    (is (= 3 (count (limit 3 :users))))
    (is (= 6 (count (limit 12 :users))))
    (is (= 4 (count (skip 2 :users))))
    (is (= 0 (count (skip 6 :users))))
    (is (= 0 (count (skip 7 :users))))
    (is (= 3 (count (skip 2 (limit 3 :users)))))
    (is (= 2 (count (skip 4 (limit 10 :users)))))
    (is (= 0 (count (limit 1 (skip 6 :users)))))
    (is (= 3 (count (limit 3 (restrict :age {:$gte 20} :users)))))
    (is (= 4 (count (limit 6 (restrict :age {:$gte 20} :users)))))
    (is (= 2 (count (skip 2 (restrict :age {:$gte 20} :users)))))
    (is (= 0 (count (skip 4 (restrict :age {:$gte 20} :users)))))
    (is (= 1 (count (limit 1 (skip 2 (restrict :age {:$gte 20} :users))))))
    (is (= 2 (count (limit 3 (skip 2 (restrict :age {:$gte 20} :users))))))
    (testing "Lazy seq"
      (let [users (query :users)]
        (is (= 6 (count users)))
        (is (not (realized? users)))
        (doall users)
        (is (= 6 (count users)))
        (is (realized? users)))
      (let [adults (restrict :age {:$gte 20} :users)]
        (is (= 4 (count adults)))
        (is (not (realized? adults)))
        (insert! :users {:name "J" :age 48})
        (is (= 5 (count adults)))
        (is (not (realized? adults)))
        (doall adults)
        (is (= 5 (count adults)))
        (is (realized? adults))
        (insert! :users {:name "Julian" :age 21})
        (is (= 5 (count adults)))))))

(deftest* insert!-test
  (with-test-db-binding
    (let [banana (insert! :items {:name "Banana" :type "Fruit" :price 120})]
      (is (instance? ObjectId (:_id banana)))
      (is (= {:_id (:_id banana) :name "Banana" :type "Fruit" :price 120} banana))
      (is (= [banana] (query :items)))
      (let [beer (insert! :items {:name "Beer" :type "Drink" :price 200})]
        (is (instance? ObjectId (:_id beer)))
        (is (= {:_id (:_id beer) :name "Beer" :type "Drink" :price 200} beer))
        (is (= [banana beer] (order :price :asc :items)))
        (let [[cola apple] (insert-multi! :items
                                          {:name "Cola" :type "Drink" :price 100}
                                          {:name "Apple" :type "Fruit" :price 80})]
          (is (instance? ObjectId (:_id cola)))
          (is (= {:_id (:_id cola) :name "Cola" :type "Drink" :price 100} cola))
          (is (instance? ObjectId (:_id apple)))
          (is (= {:_id (:_id apple) :name "Apple" :type "Fruit" :price 80} apple))
          (is (= [beer banana cola apple] (order :price :desc :items))))))
    (let [[yellow red blue] (insert-multi! :colors
                                           {:name "Yellow" :rgb {:red 1.0 :green 1.0 :blue 0.0}}
                                           {:name "Red" :rgb {:red 1.0 :green 0.0 :blue 0.0}}
                                           {:name "Blue" :rgb {:red 0.0 :green 0.0 :blue 1.0}})]
      (is (instance? ObjectId (:_id yellow)))
      (is (= {:_id (:_id yellow) :name "Yellow" :rgb {:red 1.0 :green 1.0 :blue 0.0}} yellow))
      (is (instance? ObjectId (:_id red)))
      (is (= {:_id (:_id red) :name "Red" :rgb {:red 1.0 :green 0.0 :blue 0.0}} red))
      (is (instance? ObjectId (:_id blue)))
      (is (= {:_id (:_id blue) :name "Blue" :rgb {:red 0.0 :green 0.0 :blue 1.0}} blue))
      (is (= [blue red yellow] (order :name :asc :colors)))
      (let [blue-ship (insert! :ships {:name "Bluw ship" :size :large :color blue})
            yellow-submarine (insert! :ships {:name "Yellow Submarine" :size :medium :color yellow})]
        (is (instance? ObjectId (:_id blue-ship)))
        (is (= {:_id (:_id blue-ship) :name "Blue ship" :size "large" :color blue}))
        (is (instance? ObjectId (:_id yellow-submarine)))
        (is (= {:_id (:_id yellow-submarine) :name "Yellow Submarine" :size "medium" :color yellow}))
        (is (= [blue-ship yellow-submarine] (order :name :asc :ships)))))
    (let [campari (insert! (db-collection :liquors) {:name "Campari" :color :red})
          cointreau (insert! (db-collection :liquors) {:name "Cointreau" :color :white})]
      (is (= [campari cointreau] (order :name :asc :liquors)))
      (let [[kahlua get27] (insert-multi! (db-collection :liquors) {:name "Kahlua" :color :black} {:name "GET 27" :color :green})]
        (is (= [campari cointreau get27 kahlua] (order :name :asc :liquors)))))))

(deftest* delete-one!-test
  (with-test-db-binding
    (let [item1 (insert! :items {:name "Test1" :type "Sample" :price 10})
          item2 (insert! :items {:name "Test2" :type "Test" :price 20})
          item3 (insert! :items {:name "Test3" :type "Sample" :price 30})
          item4 (insert! :items {:name "Test4" :type "Test" :price 40})]
      (is (= 4 (count (query :items))))
      (is (= item2 (delete-one! (restrict :price 20 :items))))
      (is (= 3 (count (query :items))))
      (is (= [item1 item3 item4] (order :price :asc :items)))
      (is (= (dissoc item3 :type :price) (delete-one! (project :name (order :price :desc (restrict :type "Sample" :items))))))
      (is (= 2 (count (query :items))))
      (is (= [item1 item4] (order :price :asc :items)))
      (is (= nil (delete-one! (restrict :price {> 50} :items))))
      (is (= 2 (count (query :items)))))))

(deftest* delete!-test
  (with-test-db-binding
    (insert-multi! :items
                   {:name "Test1" :type "Test" :price 10}
                   {:name "Test2" :type "Test" :price 20}
                   {:name "Test3" :type "Test" :price 30})
    (is (= 3 (count (query :items))))
    (delete! (query :items))
    (is (empty? (query :items)))
    (delete! (query :items))
    (is (empty? (query :items)))
    (let [banana (insert! :items {:name "Banana" :type "Fruit" :price 120})
          mikan (insert! :items {:name "Mikan" :type "Fruit" :price 60})
          apple (insert! :items {:name "Apple" :type "Fruit" :price 80})
          cola (insert! :items {:name "Cola" :type "Drink" :price 110})
          beer (insert! :items {:name "Beer" :type "Drink" :price 200})]
      (delete! (restrict :_id (:_id banana) :items))
      (is (= [mikan apple cola beer] (order :price :asc :items)))
      (delete! (restrict :price {:$gt 100} :items))
      (is (= [mikan apple] (order :price :asc :items)))
      (delete! (query :items))
      (is (empty? (query :items))))
    (let [jack (insert! :users {:name "Jack" :age 30})
          joel (insert! :users {:name "Joel" :age 23})
          james (insert! :users {:name "James" :age 27})
          jimmy (insert! :users {:name "Jimmy" :age 22})]
      (delete! (restrict :age {:$lt 25} :users))
      (is (= [james jack] (order :age :asc :users)))
      (is (thrown? UnsupportedOperationException (delete! (skip 1 (order :age :asc :users)))))
      (is (thrown? UnsupportedOperationException (delete! (limit 1 (order :age :asc :users))))))))

(deftest* update!-test
  (with-test-db-binding
    (let [banana (insert! :items {:name "Banana" :type "Fruit" :price 120})
          mikan (insert! :items {:name "Mikan" :type "Fruit" :price 60})
          apple (insert! :items {:name "Apple" :type "Fruit" :price 80})
          cola (insert! :items {:name "Cola" :type "Drink" :price 110})
          beer (insert! :items {:name "Beer" :type "Drink" :price 200})]
      (let [ringo (update! :$set {:name "Ringo"} (restrict :_id (:_id apple) :items))]
        (is (= {:_id (:_id apple) :name "Ringo" :type "Fruit" :price 80}
               ringo))
        (is (= [mikan ringo cola banana beer]
               (order :price :asc :items)))
        (let [orange (update! :$set {:name "Orange"} (restrict :name "Mikan" :items))]
          (is (= {:_id (:_id mikan) :name "Orange" :type "Fruit" :price 60}
                 orange))
          (is (= [orange ringo cola banana beer]
                 (order :price :asc :items)))
          (let [new-cola (update! :$inc {:price 20} (order :price :asc (restrict :type "Drink" :items)))]
            (is (= {:_id (:_id cola) :name "Cola" :type "Drink" :price 130}
                   new-cola))
            (is (= [orange ringo banana new-cola beer]
                   (order :price :asc :items)))
            (let [melon (update! :name "Melon" :type "Fruit" :price 500 (restrict :_id (:_id banana) :items))]
              (is (= {:_id (:_id banana) :name "Melon" :type "Fruit" :price 500}
                     melon))
              (is (= [orange ringo new-cola beer melon]
                     (order :price :asc :items))))))))
    (let [[chicken beef pork] (insert-multi! :meats
                                             {:name "Chicken" :price 90}
                                             {:name "Beef" :price 200}
                                             {:name "Pork" :price 130})
          expiry-date (.getTime (doto (Calendar/getInstance)
                                  (.set 2000 5 10 20 30 15)))]
      (let [new-pork (update! :$set {:price 160 :expiry expiry-date} (restrict :name "Pork" :meats))]
        (is (= {:_id (:_id pork) :name "Pork" :price 160 :expiry expiry-date}
               new-pork))
        (is (= [chicken new-pork beef]
               (order :price :asc :meats)))
        (is (nil? (update! :name "Alien" :price 3000 (restrict :name "Alien" :meats))))
        (is (= [chicken new-pork beef]
               (order :price :asc :meats)))))))

(deftest* upsert!-test
  (with-test-db-binding
    (let [banana (insert! :items {:name "Banana" :type "Fruit" :price 120})
          mikan (insert! :items {:name "Mikan" :type "Fruit" :price 60})
          apple (insert! :items {:name "Apple" :type "Fruit" :price 80})
          cola (insert! :items {:name "Cola" :type "Drink" :price 110})
          beer (insert! :items {:name "Beer" :type "Drink" :price 200})]
      (let [ringo (upsert! :$set {:name "Ringo"} (restrict :_id (:_id apple) :items))]
        (is (= {:_id (:_id apple) :name "Ringo" :type "Fruit" :price 80}
               ringo))
        (is (= [mikan ringo cola banana beer]
               (order :price :asc :items)))
        (let [orange (upsert! :$set {:name "Orange"} (restrict :name "Mikan" :items))]
          (is (= {:_id (:_id mikan) :name "Orange" :type "Fruit" :price 60}
                 orange))
          (is (= [orange ringo cola banana beer]
                 (order :price :asc :items)))
          (let [new-cola (upsert! :$inc {:price 20} (order :price :asc (restrict :type "Drink" :items)))]
            (is (= {:_id (:_id cola) :name "Cola" :type "Drink" :price 130}
                   new-cola))
            (is (= [orange ringo banana new-cola beer]
                   (order :price :asc :items)))
            (let [melon (upsert! :name "Melon" :type "Fruit" :price 500 (restrict :_id (:_id banana) :items))]
              (is (= {:_id (:_id banana) :name "Melon" :type "Fruit" :price 500}
                     melon))
              (is (= [orange ringo new-cola beer melon]
                     (order :price :asc :items))))))))
    (let [[chicken beef pork] (insert-multi! :meats
                                             {:name "Chicken" :price 90}
                                             {:name "Beef" :price 200}
                                             {:name "Pork" :price 130})
          expiry-date (.getTime (doto (Calendar/getInstance)
                                  (.set 2000 5 10 20 30 15)))]
      (let [new-pork (upsert! :$set {:price 160 :expiry expiry-date} (restrict :name "Pork" :meats))]
        (is (= {:_id (:_id pork) :name "Pork" :price 160 :expiry expiry-date}
               new-pork))
        (is (= [chicken new-pork beef]
               (order :price :asc :meats)))
        (let [alien (upsert! :name "Alien" :price 3000 (restrict :name "Alien" :meats))]
          (is (= {:_id (:_id alien) :name "Alien" :price 3000}
                 alien))
          (is (= [chicken new-pork beef alien]
                 (order :price :asc :meats))))))))

(deftest* update-multi!-test
  (with-test-db-binding
    (let [banana (insert! :items {:name "Banana" :type "Fruit" :price 120})
          mikan (insert! :items {:name "Mikan" :type "Fruit" :price 60})
          apple (insert! :items {:name "Apple" :type "Fruit" :price 80})
          cola (insert! :items {:name "Cola" :type "Drink" :price 110})
          beer (insert! :items {:name "Beer" :type "Drink" :price 200})]
      (is (= 1 (update-multi! :$set {:name "Ringo"} (restrict :_id (:_id apple) :items))))
      (let [ringo (assoc apple :name "Ringo")]
        (is (= [mikan ringo cola banana beer]
               (order :price :asc :items)))
        (is (= 1 (update-multi! :$set {:name "Orange" :price 70} (restrict :name "Mikan" :items))))
        (let [orange (assoc mikan :name "Orange" :price 70)]
          (is (= [orange ringo cola banana beer]
                 (order :price :asc :items)))
          (is (= 2 (update-multi! :$inc {:price 20} (restrict :type "Drink" :items))))
          (let [new-cola (assoc cola :price 130)
                new-beer (assoc beer :price 220)]
            (is (= [orange ringo banana new-cola new-beer]
                   (order :price :asc :items)))
            (is (= 1 (update-multi! :$inc {:price 20} (limit 1 (order :price :desc (restrict :type "Drink" :items))))))
            (let [new-beer (assoc beer :price 240)]
              (is (= [orange ringo banana new-cola new-beer]
                     (order :price :asc :items))))))))
    (let [[chicken beef pork] (insert-multi! :meats
                                             {:name "Chicken" :price 90}
                                             {:name "Beef" :price 200}
                                             {:name "Pork" :price 130})
          expiry-date (.getTime (doto (Calendar/getInstance)
                                  (.set 2000 5 10 20 30 15)))]
      (is (= 1 (update-multi! :$set {:price 160 :expiry expiry-date} (restrict :name "Pork" :meats))))
      (let [new-pork (assoc pork :price 160 :expiry expiry-date)]
        (is (= [chicken new-pork beef]
               (order :price :asc :meats)))
        (is (= 0 (update-multi! :$set {:name "Alien" :price 3000} (restrict :name "Alien" :meats))))
        (is (= [chicken new-pork beef]
               (order :price :asc :meats)))
        (is (= 0 (update-multi! :$set {:name "Alien" :price 3000} (limit 1 (order :price :desc (restrict :name "Alien" :meats))))))
        (is (= [chicken new-pork beef]
               (order :price :asc :meats)))))
    (let [yellow (insert! :colors {:name "Yellow" :score 100 :enabled true})
          red (insert! :colors {:name "Red" :score 40 :enabled true})
          green (insert! :colors {:name "Green" :score 180 :enabled true})
          blue (insert! :colors {:name "Blue" :score 120 :enabled false})]
      (is (= 2 (update-multi! :$set {:discount true} (restrict :enabled true (restrict :$isolated true (restrict :score {>= 100} :colors)))))))))

(deftest* upsert-multi!-test
  (with-test-db-binding
    (let [banana (insert! :items {:name "Banana" :type "Fruit" :price 120})
          mikan (insert! :items {:name "Mikan" :type "Fruit" :price 60})
          apple (insert! :items {:name "Apple" :type "Fruit" :price 80})
          cola (insert! :items {:name "Cola" :type "Drink" :price 110})
          beer (insert! :items {:name "Beer" :type "Drink" :price 200})]
      (is (= 1 (upsert-multi! :$set {:name "Ringo"} (restrict :_id (:_id apple) :items))))
      (let [ringo (assoc apple :name "Ringo")]
        (is (= [mikan ringo cola banana beer]
               (order :price :asc :items)))
        (is (= 1 (upsert-multi! :$set {:name "Orange" :price 70} (restrict :name "Mikan" :items))))
        (let [orange (assoc mikan :name "Orange" :price 70)]
          (is (= [orange ringo cola banana beer]
                 (order :price :asc :items)))
          (is (= 2 (upsert-multi! :$inc {:price 20} (restrict :type "Drink" :items))))
          (let [new-cola (assoc cola :price 130)
                new-beer (assoc beer :price 220)]
            (is (= [orange ringo banana new-cola new-beer]
                   (order :price :asc :items)))
            (is (= 1 (upsert-multi! :$inc {:price 20} (limit 1 (order :price :desc (restrict :type "Drink" :items))))))
            (let [new-beer (assoc beer :price 240)]
              (is (= [orange ringo banana new-cola new-beer]
                     (order :price :asc :items))))))))
    (let [[chicken beef pork] (insert-multi! :meats
                                             {:name "Chicken" :price 90}
                                             {:name "Beef" :price 200}
                                             {:name "Pork" :price 130})
          expiry-date (.getTime (doto (Calendar/getInstance)
                                  (.set 2000 5 10 20 30 15)))]
      (is (= 1 (upsert-multi! :$set {:price 160 :expiry expiry-date} (restrict :name "Pork" :meats))))
      (let [new-pork (assoc pork :price 160 :expiry expiry-date)]
        (is (= [chicken new-pork beef]
               (order :price :asc :meats)))
        (is (= 1 (upsert-multi! :$set {:name "Alien" :price 3000} (restrict :name "Alien" :meats))))
        (let [alien (fetch-one (restrict :name "Alien" :meats))]
          (is (= "Alien" (:name alien)))
          (is (= 3000 (:price alien)))
          (is (= [chicken new-pork beef alien]
                 (order :price :asc :meats)))
          (is (= 1 (upsert-multi! :$set {:name "Unknown" :price 5000} (limit 1 (order :price :desc (restrict :name "Unknown" :meats))))))
          (let [unknown (fetch-one (restrict :name "Unknown" :meats))]
            (is (= "Unknown" (:name unknown)))
            (is (= 5000 (:price unknown)))
            (is (= [chicken new-pork beef alien unknown]
                   (order :price :asc :meats)))))))))

(deftest* map-reduce!-test
  (with-test-db-binding
    (ensure-index! :users {:rank :asc} :name :user-rank)
    (ensure-index! :users {:name :asc} :unique true)
    (insert-multi! :users
                   {:name "James" :rank "GOLD" :point 300}
                   {:name "Jack" :rank "GOLD" :point 1200}
                   {:name "John" :rank "GOLD" :point 2070}
                   {:name "Sato" :rank "SILVER" :point 1021}
                   {:name "Suzuki" :rank "SILVER" :point 324})
    (ensure-index! :items {:category :asc})
    (ensure-index! :items {:price :desc})
    (insert-multi! :items
                   {:name "Banan" :price 120 :category "Fruit" :enabled true}
                   {:name "Apple" :price 80 :category "Fruit" :enabled false}
                   {:name "Grape" :price 130 :category "Fruit" :enabled true}
                   {:name "Orange" :price 90 :category "Fruit" :enabled false}
                   {:name "Beer" :price 300 :category "Drink" :enabled true}
                   {:name "Tea" :price 200 :category "Drink" :enabled true}
                   {:name "Water" :price 100 :category "Drink" :enabled false})
    (testing "Map/Reduce without restriction, limit and sorting"
      (map-reduce! :map "function() {emit(this.rank, this.point);}"
                   :reduce "function(key, vals) {var sum = 0; for (var i = 0; i < vals.length; ++i) sum += vals[i]; return sum;}"
                   :out :total-point-by-rank
                   :users)
      (is (= [{:_id "GOLD" :value 3570.0} {:_id "SILVER" :value 1345.0}]
             (order :_id :asc :total-point-by-rank)))
      (map-reduce! :map "function() {emit(this.category, this.price);}"
                   :reduce "function(key, vals) {var sum = 0; for (var i = 0; i < vals.length; ++i) sum += vals[i]; return sum;}"
                   :out :total-price-by-category
                   :items)
      (is (= [{:_id "Drink" :value 600.0} {:_id "Fruit" :value 420.0}]
             (order :_id :asc :total-price-by-category))))
    (testing "Map/Reduce with finalize"
      (map-reduce! :map "function() {emit(this.rank, {point: this.point, count: 1});}"
                   :reduce "function(key, vals) {var sum = {point: 0, count: 0}; for (var i = 0; i < vals.length; ++i) sum = {point: sum.point + vals[i].point, count: sum.count + vals[i].count}; return sum;}"
                   :finalize "function(key, val) {return val.point / val.count;}"
                   :out :average-of-points-by-rank
                   :users)
      (is (= [{:_id "GOLD" :value 1190.0} {:_id "SILVER" :value 672.5}]
             (order :_id :asc :average-of-points-by-rank))))
    (testing "Map/Reduce with restriction"
      (map-reduce! :map "function() {emit(this.category, this.price);}"
                   :reduce "function(key, vals) {var sum = 0; for (var i = 0; i < vals.length; ++i) sum += vals[i]; return sum;}"
                   :out :total-enabled-price-by-category
                   (restrict :enabled true :items))
      (is (= [{:_id "Drink" :value 500.0} {:_id "Fruit" :value 250.0}]
             (order :_id :asc :total-enabled-price-by-category))))
    (testing "Map/Reduce with sort and limit"
      (map-reduce! :map "function() {emit(this.category, this.price);}"
                   :reduce "function(key, vals) {var sum = 0; for (var i = 0; i < vals.length; ++i) sum += vals[i]; return sum;}"
                   :out :total-price-of-top5-by-category
                   (limit 5 (order :price :desc :items)))
      (is (= [{:_id "Drink" :value 600.0} {:_id "Fruit" :value 250.0}]
             (order :_id :asc :total-price-of-top5-by-category))))
    (testing "Map/Reduce with restriction, sort and limit"
      (map-reduce! :map "function() {emit(this.category, this.price);}"
                   :reduce "function(key, vals) {var sum = 0; for (var i = 0; i < vals.length; ++i) sum += vals[i]; return sum;}"
                   :out :total-price-of-inexpensive3-by-category
                   (limit 3 (order :price :asc (restrict :enabled true :items))))
      (is (= [{:_id "Drink" :value 200.0} {:_id "Fruit" :value 250.0}])
          (order :_id :asc :total-price-of-inexpensive3-by-category)))
    (testing "Map/Reduce with out-types"
      (map-reduce! :map "function() {emit(this.category, this.price);}"
                   :reduce "function(key, vals) {var sum = 0; for (var i = 0; i < vals.length; ++i) sum += vals[i]; return sum;}"
                   :out :out-types-test
                   :items)
      (is (= [{:_id "Drink" :value 600.0} {:_id "Fruit" :value 420.0}]
             (order :_id :asc :out-types-test)))
      (map-reduce! :map "function() {emit(this.category, this.price);}"
                   :reduce "function(key, vals) {var sum = 0; for (var i = 0; i < vals.length; ++i) sum += vals[i]; return sum;}"
                   :out :out-types-test
                   :out-type :replace
                   (restrict :category "Fruit" :items))
      (is (= [{:_id "Fruit" :value 420.0}]
             (order :_id :asc :out-types-test)))
      (map-reduce! :map "function() {emit(this.category, this.price);}"
                   :reduce "function(key, vals) {var sum = 0; for (var i = 0; i < vals.length; ++i) sum += vals[i]; return sum;}"
                   :out :out-types-test
                   :out-type :merge
                   (restrict :category "Drink" :items))
      (is (= [{:_id "Drink" :value 600.0} {:_id "Fruit" :value 420.0}]
             (order :_id :asc :out-types-test)))
      (map-reduce! :map "function() {emit(this.category, this.price);}"
                   :reduce "function(key, vals) {var sum = 0; for (var i = 0; i < vals.length; ++i) sum += vals[i]; return sum;}"
                   :out :out-types-test
                   :out-type :reduce
                   :items)
      (is (= [{:_id "Drink" :value 1200.0} {:_id "Fruit" :value 840.0}]
             (order :_id :asc :out-types-test)))
      (map-reduce! :map "function() {emit(this.category, this.price);}"
                   :reduce "function(key, vals) {var sum = 0; for (var i = 0; i < vals.length; ++i) sum += vals[i]; return sum;}"
                   :out :out-types-test
                   :out-type :merge
                   :verbose false
                   (restrict :category "Fruit" :items))
      (is (= [{:_id "Drink" :value 1200.0} {:_id "Fruit" :value 420.0}]
             (order :_id :asc :out-types-test))))
    (testing "Map/Reduce with scope"
      (map-reduce! :map "function() {emit(this.rank, this.point * rate);}"
                   :reduce "function(key, vals) {var sum = 0; for (var i = 0; i < vals.length; ++i) sum += vals[i]; return sum;}"
                   :scope {:rate 1.3}
                   :out :total-of-points-by-rank
                   :verbose true
                   :users)
      (is (= [{:_id "GOLD" :value 4641.0} {:_id "SILVER" :value 1748.5}]
             (order :_id :asc :total-of-points-by-rank))))))

(deftest* grid-fs-test
  (with-test-db-binding
    (let [fs (grid-fs)]
      (is (instance? GridFS fs))
      (is (= *db* (.getDB fs)))
      (is (= "fs" (.getBucketName fs))))
    (let [test-fs (grid-fs "test-fs")]
      (is (instance? GridFS test-fs))
      (is (= *db* (.getDB test-fs)))
      (is (= "test-fs" (.getBucketName test-fs))))
    (let [image-fs (grid-fs "image-fs")]
      (is (instance? GridFS image-fs))
      (is (= *db* (.getDB image-fs)))
      (is (= "image-fs" (.getBucketName image-fs))))))

(defn- bytes<-input-stream [input]
  (let [buffer-size 100]
    (lazy-seq (let [bytes (byte-array buffer-size)
                    read-length (.read input bytes)]
                (if (= -1 read-length)
                  nil
                  (concat (take read-length bytes)
                          (bytes<-input-stream input)))))))

(deftest* simple-grid-fs-insert-and-file-test
  (with-test-db-binding
    (let [data (ByteArrayInputStream. (byte-array []))
          empty-file (insert! (grid-fs :test-fs) {:data data})
          expected {:aliases nil
                    :uploadDate (:uploadDate empty-file)
                    :contentType nil
                    :filename nil
                    :md5 "d41d8cd98f00b204e9800998ecf8427e"
                    :length 0
                    :chunkSize (:chunkSize empty-file)
                    :_id (:_id empty-file)}]
      (is (instance? ObjectId (:_id empty-file)))
      (is (instance? Date (:uploadDate empty-file)))
      (is (integer? (:chunkSize empty-file)))
      (is (= (assoc expected :data data) empty-file))
      (is (= expected (dissoc (fetch-one (grid-fs :test-fs)) :data)))
      (is (= [] (bytes<-input-stream (:data (fetch-one (grid-fs :test-fs)))))))
    (let [data (into-array Byte/TYPE [0 1 2 3 4 5 6])
          test-file (insert! (grid-fs :test-fs-2)
                             {:data data
                              :filename "test_image.png"
                              :contentType "image/png"
                              :chunkSize 10000
                              :aliases ["default.png" "default2.png"]
                              :metadata {:owner "Smith" :size {:width 480 :height 320}}
                              :other-data "test"})
          expected {:aliases ["default.png" "default2.png"]
                    :uploadDate (:uploadDate test-file)
                    :contentType "image/png"
                    :filename "test_image.png"
                    :md5 "9aa461e1eca4086f9230aa49c90b0c61"
                    :length 7
                    :chunkSize 10000
                    :metadata {:owner "Smith" :size {:width 480 :height 320}}
                    :other-data "test"
                    :_id (:_id test-file)}]
      (is (instance? ObjectId (:_id test-file)))
      (is (instance? Date (:uploadDate test-file)))
      (is (= (assoc expected :data data) test-file))
      (is (= expected (dissoc (fetch-one (grid-fs :test-fs-2)) :data)))
      (is (= [0 1 2 3 4 5 6] (bytes<-input-stream (:data (fetch-one (grid-fs :test-fs-2)))))))))

(deftest* grid-fs-query-test
  (with-test-db-binding
    (is (empty? (query (grid-fs :images))))
    (let [image1-data (range Byte/MIN_VALUE (inc Byte/MAX_VALUE))
          image1 (insert! (grid-fs :images) {:data (into-array Byte/TYPE image1-data)
                                             :filename "image1.jpg"
                                             :contentType "image/jpeg"
                                             :aliases ["image-one"]
                                             :rank 30
                                             :metadata {:size {:width 200 :height 400}}})
          image1-expected (assoc image1 :data image1-data)
          image2-data (repeat 500 10)
          image2 (insert! (grid-fs :images) {:data (into-array Byte/TYPE image2-data)
                                             :filename "image2.png"
                                             :contentType "image/png"
                                             :aliases ["image-two"]
                                             :chunkSize 100
                                             :rank 40
                                             :metadata {:size {:width 200 :height 300}}})
          image2-expected (assoc image2 :data image2-data)
          image3-data (range 10)
          image3 (insert! (grid-fs :images) {:data (into-array Byte/TYPE image3-data)
                                             :filename "image3.png"
                                             :chunkSize 10
                                             :rank 20
                                             :contentType "image/png"})
          image3-expected (assoc image3 :data image3-data)]
      (testing "Order"
        (is (= [image1-expected image2-expected image3-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (order :filename :asc (grid-fs :images)))))
        (is (= [image3-expected image2-expected image1-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (order :filename :desc (grid-fs :images)))))
        (is (= [image3-expected image2-expected image1-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (order :metadata.size.height :asc (grid-fs :images)))))
        (is (= [image1-expected image2-expected image3-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (order :metadata.size.height :desc (grid-fs :images)))))
        (is (= [image3-expected image1-expected image2-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (order :rank :asc (grid-fs :images))))))
      (testing "Restriction"
        (is (= [image2-expected image3-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (order :filename :asc (restrict :contentType "image/png" (grid-fs :images))))))
        (is (= [image1-expected image2-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (order :filename :asc (restrict :metadata.size.width 200 (grid-fs :images))))))
        (is (= [image3-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (restrict :rank {:$lt 25} (grid-fs :images))))))
      (testing "Limit"
        (is (= [image1-expected image2-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (limit 2 (order :filename :asc (grid-fs :images))))))
        (is (= [image1-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (limit 1 (order :filename :asc (grid-fs :images))))))
        (is (= [image3-expected image2-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (limit 2 (order :filename :desc (grid-fs :images))))))
        (is (= [image3-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (limit 1 (order :rank :asc (restrict :contentType "image/png" (grid-fs :images)))))))
        (is (= [image1-expected image2-expected image3-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (limit 10 (order :filename :asc (grid-fs :images)))))))
      (testing "Skip"
        (is (= [image2-expected image3-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (skip 1 (order :filename :asc (grid-fs :images))))))
        (is (= [image2-expected image1-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (skip 1 (order :filename :desc (grid-fs :images))))))
        (is (= [image3-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (skip 2 (order :rank :desc (grid-fs :images))))))
        (is (= [image2-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (skip 2 (order :rank :asc (grid-fs :images))))))
        (is (= [image2-expected]
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (skip 1 (order :rank :asc (restrict :contentType "image/png" (grid-fs :images)))))))
        (is (= []
               (map #(assoc % :data (bytes<-input-stream (:data %)))
                    (skip 5 (grid-fs :images))))))
      (testing "Fetch one"
        (is (= image1-expected
               (#(assoc % :data (bytes<-input-stream (:data %)))
                (fetch-one (order :filename :asc (grid-fs :images))))))
        (is (= image2-expected
               (#(assoc % :data (bytes<-input-stream (:data %)))
                (fetch-one (restrict :_id (:_id image2) (grid-fs :images))))))
        (is (= image1-expected
               (#(assoc % :data (bytes<-input-stream (:data %)))
                (fetch-one (limit 2 (order :filename :asc (grid-fs :images)))))))
        (is (= image3-expected
               (#(assoc % :data (bytes<-input-stream (:data %)))
                (fetch-one (skip 2 (order :filename :asc (grid-fs :images)))))))
        (is (nil? (fetch-one (restrict :contentType "text/plain" (grid-fs :images))))))
      (testing ":data field"
        (let [file (fetch-one (restrict :_id (:_id image1) (grid-fs :images)))]
          (is (instance? InputStream (:data file))))))))

(deftest* grid-fs-insert-test
  (with-test-db-binding
    (let [data1 (range 0 100)
          data1-bytes (into-array Byte/TYPE data1)
          file1-properties {:filename "test1"
                            :contentType "binary"
                            :aliases ["test-one" "test"]
                            :chunkSize 50
                            :metadata {:author "Jack"}}
          file1 (insert! (grid-fs :test-files) (assoc file1-properties :data data1-bytes))
          file1-expected (assoc file1-properties
                           :data data1-bytes
                           :_id (:_id file1)
                           :uploadDate (:uploadDate file1)
                           :length 100
                           :md5 "7acedd1a84a4cfcb6e7a16003242945e")]
      (is (= 1 (count (query (grid-fs :test-files)))))
      (is (instance? ObjectId (:_id file1)))
      (is (instance? Date (:uploadDate file1)))
      (is (= file1-expected file1))
      (is (= (assoc file1-expected :data data1)
             (#(assoc % :data (bytes<-input-stream (:data %)))
              (fetch-one (restrict :_id (:_id file1) (grid-fs :test-files)))))))
    (let [data2 (range 30 70)
          data2-iter (ByteArrayInputStream. (into-array Byte/TYPE data2))
          file2-properties {:filename "test-file2"}
          file2 (insert! (grid-fs :test-files) (assoc file2-properties :data data2-iter))
          file2-expected (assoc file2-properties
                           :data data2-iter
                           :_id (:_id file2)
                           :contentType nil
                           :length 40
                           :uploadDate (:uploadDate file2)
                           :aliases nil
                           :chunkSize GridFS/DEFAULT_CHUNKSIZE
                           :md5 "6277c67ad00eab601d7885f88877558b")]
      (is (= 2 (count (query (grid-fs :test-files)))))
      (is (instance? ObjectId (:_id file2)))
      (is (instance? Date (:uploadDate file2)))
      (is (= file2-expected file2))
      (is (= (assoc file2-expected :data data2)
             (#(assoc % :data (bytes<-input-stream (:data %)))
              (fetch-one (restrict :_id (:_id file2) (grid-fs :test-files)))))))
    (let [data3 (range -100 100)
          data3-bytes (into-array Byte/TYPE data3)
          file3-properties {:filename "file3"
                            :contentType "image/png"
                            :metadata {:size {:width 340 :height 180}}}
          data4 (repeat 100 1)
          data4-bytes (into-array Byte/TYPE data4)
          file4-properties {}
          [file3 file4] (insert-multi! (grid-fs :test-files)
                                       (assoc file3-properties
                                         :data data3-bytes)
                                       (assoc file4-properties
                                         :data data4-bytes))
          file3-expected (assoc file3-properties
                           :data data3-bytes
                           :_id (:_id file3)
                           :length 200
                           :uploadDate (:uploadDate file3)
                           :aliases nil
                           :chunkSize GridFS/DEFAULT_CHUNKSIZE
                           :md5 "37560bac49bc38fd700f3ae4ce803c68")
          file4-expected (assoc file4-properties
                           :data data4-bytes
                           :_id (:_id file4)
                           :filename nil
                           :contentType nil
                           :length 100
                           :uploadDate (:uploadDate file4)
                           :aliases nil
                           :chunkSize GridFS/DEFAULT_CHUNKSIZE
                           :md5 "7806c0bf75f9f9b46ba74ebb8aff2de4")]
      (is (= 4 (count (query (grid-fs :test-files)))))
      (is (instance? ObjectId (:_id file3)))
      (is (instance? Date (:uploadDate file3)))
      (is (= file3-expected file3))
      (is (= (assoc file3-expected :data data3)
             (#(assoc % :data (bytes<-input-stream (:data %)))
              (fetch-one (restrict :_id (:_id file3) (grid-fs :test-files))))))
      (is (instance? ObjectId (:_id file4)))
      (is (instance? Date (:uploadDate file4)))
      (is (= file4-expected file4))
      (is (= (assoc file4-expected :data data4)
             (#(assoc % :data (bytes<-input-stream (:data %)))
              (fetch-one (restrict :_id (:_id file4) (grid-fs :test-files)))))))))

(deftest* grid-fs-delete-test
  (with-test-db-binding
    (let [[file1 file2 file3 file4] (insert-multi! (grid-fs :test-files)
                                                   {:data (into-array Byte/TYPE [1])
                                                    :filename "1"
                                                    :contentType "image/jpg"}
                                                   {:data (into-array Byte/TYPE [2])
                                                    :filename "2"
                                                    :contentType "image/png"}
                                                   {:data (into-array Byte/TYPE [3])
                                                    :filename "3"
                                                    :contentType "image/jpg"}
                                                   {:data (into-array Byte/TYPE [4])
                                                    :filename "4"
                                                    :contentType "image/png"})]
      (is (= 4 (count (query (grid-fs :test-files)))))
      (delete! (restrict :_id (:_id file3) (grid-fs :test-files)))
      (is (= 3 (count (query (grid-fs :test-files)))))
      (is (nil? (fetch-one (restrict :_id (:_id file3) (grid-fs :test-files)))))
      (is (fetch-one (restrict :_id (:_id file1) (grid-fs :test-files))))
      (is (fetch-one (restrict :_id (:_id file2) (grid-fs :test-files))))
      (is (fetch-one (restrict :_id (:_id file4) (grid-fs :test-files))))
      (delete! (restrict :contentType "image/png" (grid-fs :test-files)))
      (is (= 1 (count (query (grid-fs :test-files)))))
      (is (nil? (fetch-one (restrict :_id (:_id file2) (grid-fs :test-files)))))
      (is (nil? (fetch-one (restrict :_id (:_id file3) (grid-fs :test-files)))))
      (is (nil? (fetch-one (restrict :_id (:_id file4) (grid-fs :test-files)))))
      (is (fetch-one (restrict :_id (:_id file1) (grid-fs :test-files))))
      (delete! (grid-fs :test-files))
      (is (empty? (query (grid-fs :test-files))))
      (delete! (restrict :contentType "image/jpg" (grid-fs :test-files)))
      (is (empty? (query (grid-fs :test-files)))))))

(deftest* db-ref-test
  (with-test-db-binding
    (let [[ika tako] (insert-multi! :items
                                    {:name "Ika" :price 200}
                                    {:name "Tako" :price 190})
          ika-sale (insert! :sales {:item (db-ref :items (:_id ika)) :discount 20})
          tako-sale (insert! :sales {:item (db-ref :items (:_id tako)) :discount 40})]
      (is (instance? DBRef (:item ika-sale)))
      (is (instance? IDeref (:item ika-sale)))
      (is (instance? IPending (:item ika-sale)))
      (is (not (realized? (:item ika-sale))))
      (is (= ika @(:item ika-sale)))
      (is (realized? (:item ika-sale)))
      (is (= ika @(:item ika-sale)))
      (is (instance? DBRef (:item tako-sale)))
      (is (instance? IDeref (:item tako-sale)))
      (is (instance? IPending (:item tako-sale)))
      (is (not (realized? (:item tako-sale))))
      (is (= tako @(:item tako-sale)))
      (is (realized? (:item tako-sale)))
      (is (= tako @(:item tako-sale)))
      (let [ika-ref (:item (fetch-one (restrict :_id (:_id ika-sale) :sales)))
            tako-ref (:item (fetch-one (restrict :_id (:_id tako-sale) :sales)))]
        (is (instance? DBRef ika-ref))
        (is (instance? IDeref ika-ref))
        (is (instance? IPending ika-ref))
        (is (not (realized? ika-ref)))
        (is (= ika @ika-ref))
        (is (realized? ika-ref))
        (is (= ika @ika-ref))
        (is (instance? DBRef tako-ref))
        (is (instance? IDeref tako-ref))
        (is (instance? IPending tako-ref))
        (is (not (realized? tako-ref)))
        (is (= tako @tako-ref))
        (is (realized? tako-ref))
        (is (= tako @tako-ref))))))

(deftest* object-id?-test
  (is (object-id? (ObjectId.)))
  (is (not (object-id? (str (ObjectId.)))))
  (is (not (object-id? nil)))
  (is (not (object-id? 123890))))

(deftest* object-id<--test
  (let [object-id (ObjectId.)]
    (is (= object-id (object-id<- object-id)))
    (is (= object-id (object-id<- (str object-id)))))
  (is (nil? (object-id<- nil)))
  (is (nil? (object-id<- "iuefgiauofhia")))
  (is (nil? (object-id<- 3123456157))))

(deftest* preserve-meta-test
  (with-test-db-binding
    (insert-multi! :fruits {:name "Banana" :price 200} {:name "Apple" :price 100})
    (is (= "fruits" (-> (with-meta (query :fruits) {::name "fruits"})
                        meta
                        ::name)))
    (is (= "fruits2" (->> (with-meta (query :fruits) {::name "fruits2"})
                          (restrict :name "Banana")
                          meta
                          ::name)))
    (is (= "fruits3" (->> (with-meta (query :fruits) {::label "fruits3"})
                          (project :name)
                          meta
                          ::label)))
    (is (= "fruits4" (->> (with-meta (query :fruits) {::label "fruits4"})
                          (order :price :asc)
                          meta
                          ::label)))
    (is (= "fruits5" (->> (with-meta (query :fruits) {::tag "fruits5"})
                          (skip 1)
                          meta
                          ::tag)))
    (is (= "fruits6" (->> (with-meta (query :fruits) {::tag "fruits6"})
                          (limit 1)
                          meta
                          ::tag)))
    (is (= "fruits7" (->> (with-meta (query :fruits) {::key "fruits7"})
                          (map-after #(assoc % :tax (* (:price %) 0.1)))
                          meta
                          ::key)))))

(use-fixtures :each #(time (%)))

;; (time (run-tests))
