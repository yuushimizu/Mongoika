(ns mongoika
  (use [mongoika
        [conversion :only [mongo-object<- <-mongo-object keyword<- str<-]]])
  (require [mongoika
            [proper-mongodb-collection :as proper-mongodb-collection]
            [params :as params]
            [query :as query]
            [request :as request]
            [db-ref :as db-ref]])
  (import [clojure.lang IPersistentMap Sequential Named]
          [java.lang.reflect Field]
          [com.mongodb Mongo ServerAddress MongoOptions DB DBCollection DBObject WriteResult CommandResult DBRef]
          [com.mongodb.gridfs GridFS]
          [org.bson.types ObjectId]))

(defprotocol MongoResult
  (throw-on-error [result]))

(extend-protocol MongoResult
  CommandResult
  (throw-on-error [result]
    (or (.throwOnError ^CommandResult result) true))
  WriteResult
  (throw-on-error [result]
    (throw-on-error (.getLastError ^WriteResult result))))

(defmulti set-mongo-options-field! (fn [options field value] (.getType ^Field field)))

(defmethod set-mongo-options-field! Integer/TYPE [options field value]
  (.setInt ^Field field ^MongoOptions options ^Integer value))

(defmethod set-mongo-options-field! Boolean/TYPE [options field value]
  (.setBoolean ^Field field ^MongoOptions options ^Boolean value))

(defn- set-mongo-option! [options option-name value]
  (letfn [(method [class name param-types]
            (.getMethod ^Class class ^String name ^"[Ljava.lang.Class" (into-array Class param-types)))]
    (let [field (.getField MongoOptions ^String (name option-name))]
      (set-mongo-options-field! options (.getField MongoOptions ^String (name option-name)) value))))

(defn mongo-options [options-map]
  (let [options (MongoOptions.)]
    (doseq [[option-name value] options-map]
      (set-mongo-option! options option-name value))
    options))

(def default-host "127.0.0.1")
(def default-port 27017)

(defprotocol ToServerAddress
  (server-address [x]))
(extend-protocol ToServerAddress
  ServerAddress
  (server-address [x] x)
  String
  (server-address [host]
    (server-address {:host host}))
  IPersistentMap
  (server-address [{:keys [host port]}]
    (ServerAddress. ^String (or host default-host) ^Integer (or port default-port))))

(defn- multiple? [x]
  (and (not (map? x)) (coll? x)))

(defn- to-multiple [x]
  (if (multiple? x) x [x]))

(defn mongo
  ([address-or-addresses options]
     (let [mongo-options (if (instance? MongoOptions options)
                           options
                           (mongo-options options))
           addresses (map server-address (to-multiple address-or-addresses))]
       (if (= 1 (count addresses))
         (Mongo. ^ServerAddress (first addresses)
                 ^MongoOptions mongo-options)
         (Mongo. ^java.util.List addresses
                 ^MongoOptions mongo-options))))
  ([address-or-addresses]
     (mongo address-or-addresses {}))
  ([]
     (mongo [{}]))
  ([address-or-addresses option-key option-value & rest-options]
     (mongo address-or-addresses (into {option-key option-value}
                                       (apply hash-map rest-options)))))

(defn close-mongo [mongo]
  (.close ^Mongo mongo))

(defmacro with-mongo [[mongo-var & args] & body]
  `(let [mongo# (mongo ~@args)
         ~mongo-var mongo#]
     (try
       ~@body
       (finally (close-mongo mongo#)))))

(defn database [mongo db-name]
  (.getDB ^Mongo mongo ^String (name db-name)))

(defmacro with-request [& body]
  `(request/with-request *db* ~@body))

(def ^{:dynamic true} *db*)

(defn set-default-db! [db]
  (alter-var-root #'*db* (constantly db)))

(defmacro with-db-binding [db & body]
  `(binding [*db* ~db]
     ~@body))

(defn bound-db []
  *db*)

(defn add-user! [user password]
  (.addUser ^DB *db* ^String user ^chars (.toCharArray ^String password)))

(defn authenticate [user password]
  (.authenticate ^DB *db* ^String user ^chars (.toCharArray ^String password)))

(defn authenticated? []
  (.isAuthenticated *db*))

(defn collection-names []
  (set (.getCollectionNames ^DB *db*)))

(defn collection-exists? [collection-name]
  (.collectionExists ^DB *db* ^String (name collection-name)))

(defprotocol DBCollectionSource
  (db-collection [this]))
(extend-protocol DBCollectionSource
  String
  (db-collection [this]
    (.getCollection ^DB *db* ^String this))
  Named
  (db-collection [this]
    (db-collection ^String (name this)))
  DBCollection
  (db-collection [this]
    this))

(defn collection-stats [collection-name]
  (<-mongo-object (.getStats (db-collection collection-name))))

(defn ensure-index! [collection orders & {:as options}]
  (.ensureIndex ^DBCollection (db-collection collection)
                ^DBObject (params/fix-param :order (if (map? orders) orders (partition-all 2 orders)))
                ^DBObject (mongo-object<- (or options {}))))

(extend-protocol query/QuerySource
  Named
  (proper-mongodb-collection [this]
    (db-collection this))
  (parameters [this]
    {})
  String
  (proper-mongodb-collection [this]
    (db-collection this))
  (parameters [this]
    {}))

(defn query-source? [x]
  (satisfies? query/QuerySource x))

(defn query [query-source]
  (query/make-query query-source))

(defn- split-last [coll]
  (loop [butlast [] [f & r] coll]
    (if (empty? r)
      [butlast f]
      (recur (conj butlast f) r))))

(defn restrict [& conditions-and-query-source]
  (let [[conditions query-source] (split-last conditions-and-query-source)]
    (query/add-param query-source :restrict (apply hash-map conditions))))

(defn project [& fields-and-query-source]
  (let [[fields query-source] (split-last fields-and-query-source)]
    (query/add-param query-source :project
                     (reduce (fn [projection [include? values]]
                               (assoc projection include? (set (map first values))))
                             {}
                             (group-by (fn [[field include?]] (boolean include?))
                                       (reduce (fn [include-values field] (into include-values (if (map? field) field {field true})))
                                               {}
                                               fields))))))

(defn order [& conditions-and-query-source]
  (let [[conditions query-source] (split-last conditions-and-query-source)]
    (query/add-param query-source :order (partition-all 2 conditions))))

(defn reverse-order [query-source]
  (query/add-param query-source :order query/order-reverse))

(defn limit [n collection]
  (if (query-source? collection)
    (query/add-param collection :limit n)
    (take n collection)))

(defn skip [n collection]
  (if (query-source? collection)
    (query/add-param collection :skip n)
    (drop n collection)))

(defn batch-size [n query-source]
  (query/add-param query-source :batch-size n))

(defn query-options [& options-and-query-source]
  (let [[options query-source] (split-last options-and-query-source)]
    (query/add-param query-source :query-options options)))

(defn postapply [f query-source]
  (query/add-param query-source :postapply f))

(defn map-after [f query-source]
  (postapply (partial map f) query-source))

(defn fetch-one [query-source]
  (proper-mongodb-collection/fetch-one (query/proper-mongodb-collection query-source) (query/parameters query-source)))

(defn insert! [query-source doc]
  (proper-mongodb-collection/insert! (query/proper-mongodb-collection query-source) (query/parameters query-source) doc))

(defn insert-multi! [query-source & docs]
  (proper-mongodb-collection/insert-multi! (query/proper-mongodb-collection query-source) (query/parameters query-source) docs))

(defn update! [& update-operations-and-query-source]
  (let [[update-operations query-source] (split-last update-operations-and-query-source)]
    (proper-mongodb-collection/update! (query/proper-mongodb-collection query-source) (query/parameters query-source) (apply hash-map update-operations))))

(defn update-multi! [& update-operations-and-query-source]
  (let [[update-operations query-source] (split-last update-operations-and-query-source)]
    (proper-mongodb-collection/update-multi! (query/proper-mongodb-collection query-source) (query/parameters query-source) (apply hash-map update-operations))))

(defn upsert! [& update-operations-and-query-source]
  (let [[update-operations query-source] (split-last update-operations-and-query-source)]
    (proper-mongodb-collection/upsert! (query/proper-mongodb-collection query-source) (query/parameters query-source) (apply hash-map update-operations))))

(defn upsert-multi! [& update-operations-and-query-source]
  (let [[update-operations query-source] (split-last update-operations-and-query-source)]
    (proper-mongodb-collection/upsert-multi! (query/proper-mongodb-collection query-source) (query/parameters query-source) (apply hash-map update-operations))))

(defn delete-one! [query-source]
  (proper-mongodb-collection/delete-one! (query/proper-mongodb-collection query-source) (query/parameters query-source)))

(defn delete! [query-source]
  (proper-mongodb-collection/delete! (query/proper-mongodb-collection query-source) (query/parameters query-source)))

(defn ^{:doc "Invoke map/reduce with query and options.

Options:
- map: map function as a JavaScript code
- reduce: reduce function as a JavaScript code
- finalize: finalize function as a JavaScript code
- out: name of collection to output to
- out-type: replace/merge/reduce
- scope: variables to use in map/reduce/finalize functions
- verbose

The query can contain restriction, limit and order."}
  map-reduce! [& options-and-query-source]
  (let [[options query-source] (split-last options-and-query-source)
        {:keys [out] :as options} (apply hash-map options)]
    (proper-mongodb-collection/map-reduce! (query/proper-mongodb-collection query-source) (query/parameters query-source) (if out (assoc options :out (query/proper-mongodb-collection out)) options))))

(defn grid-fs
  ([bucket]
     (GridFS. ^DB *db* ^String (name bucket)))
  ([]
     (GridFS. ^DB *db*)))

(defprotocol GridFSSource
  (<-grid-fs [this]))
(extend-protocol GridFSSource
  GridFS
  (<-grid-fs [this] this)
  Named
  (<-grid-fs [this]
    (grid-fs this)))

(defn db-ref [collection-name id]
  (db-ref/derefable-db-ref *db* (str<- collection-name) id))

(defn object-id? [o]
  (instance? ObjectId o))

(defn object-id<- [x]
  (if (object-id? x)
    x
    (try (ObjectId. x)
         (catch Exception _ nil))))
