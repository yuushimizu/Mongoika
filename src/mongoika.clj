(ns mongoika
  (use [mongoika
        [conversion :only [mongo-object<- <-mongo-object keyword<- str<-]]])
  (require [mongoika
            [proper-mongo-collection :as proper]
            [query :as query]
            [db-ref :as db-ref]])
  (import [clojure.lang IPersistentMap Named]
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
  (letfn [(method [class name parameter-types]
            (.getMethod ^Class class ^String name ^"[Ljava.lang.Class" (into-array Class parameter-types)))]
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

(defmacro with-request [database & body]
  `(do (.requestStart ^DB ~database)
       (try ~@body
            (finally (.requestDone ^DB ~database)))))

(def ^{:dynamic true} *db*)

(defn set-default-db! [db]
  (alter-var-root #'*db* (constantly db)))

(defmacro with-db-binding [db & body]
  `(binding [*db* ~db]
     (with-request *db*
       ~@body)))

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

(defn db-collection [collection-name]
  (.getCollection ^DB *db* ^String (name collection-name)))

(defprotocol DBCollectionSource
  (<-db-collection [this]))
(extend-protocol DBCollectionSource
  DBCollection
  (<-db-collection [this] this)
  Named
  (<-db-collection [this]
    (db-collection this)))

(extend-protocol query/MongoCollection
  Named
  (proper-mongo-collection<- [this]
    (db-collection this))
  (query-parameters [this]
    {}))

(defn query [mongo-collection]
  (query/create mongo-collection))

(defn- split-last [coll]
  (loop [butlast [] [f & r] coll]
    (if (empty? r)
      [butlast f]
      (recur (conj butlast f) r))))

(defn restrict [& conditions-and-mongo-collection]
  (let [[conditions mongo-collection] (split-last conditions-and-mongo-collection)]
    (query/add-parameter mongo-collection :restrict (apply hash-map conditions))))

(defn project [& fields-and-mongo-collection]
  (let [[fields mongo-collection] (split-last fields-and-mongo-collection)]
    (query/add-parameter mongo-collection :project fields)))

(defn- fix-order-conditions [conditions]
  (letfn [(fix-pair [[field order]]
            [field (or ({:asc 1 :desc -1} order)
                       order)])]
    (cond (map? conditions) (map fix-pair conditions)
          (empty? conditions) []
          (empty? (rest conditions)) [(first conditions) 1]
          :else (lazy-seq (cons (fix-pair (take 2 conditions))
                                (fix-order-conditions (nnext conditions)))))))

(defn order [& conditions-and-mongo-collection]
  (let [[conditions mongo-collection] (split-last conditions-and-mongo-collection)]
    (query/add-parameter mongo-collection :order (fix-order-conditions conditions))))

(defn reverse-order [mongo-collection]
  (let [current-order (:order (query/query-parameters mongo-collection))]
    (if (empty? current-order)
      (order :$natural :desc mongo-collection)
      (query/assoc-parameter mongo-collection :order (map (fn [[field order]]
                                                            [field (if (and (number? order) (neg? order)) 1 -1)])
                                                          current-order)))))

(defn limit [n mongo-collection]
  (query/add-parameter mongo-collection :limit n))

(defn skip [n mongo-collection]
  (query/add-parameter mongo-collection :skip n))

(defn batch-size [n mongo-collection]
  (query/add-parameter mongo-collection :batch-size n))

(defn query-options [& options-and-mongo-collection]
  (let [[options mongo-collection] (split-last options-and-mongo-collection)]
    (query/add-parameter mongo-collection :query-options options)))

(defn map-after [f mongo-collection]
  (query/add-parameter mongo-collection :map-after f))

(defn fetch-one [mongo-collection]
  (proper/fetch-one (query/proper-mongo-collection<- mongo-collection)
                    (query/query-parameters mongo-collection)))

(defn insert! [mongo-collection obj]
  (proper/insert! (query/proper-mongo-collection<- mongo-collection)
                  (query/query-parameters mongo-collection)
                  obj))

(defn insert-multi! [mongo-collection & objs]
  (proper/insert-multi! (query/proper-mongo-collection<- mongo-collection)
                        (query/query-parameters mongo-collection)
                        objs))

(defn delete! [mongo-collection]
  (proper/delete! (query/proper-mongo-collection<- mongo-collection)
                  (query/query-parameters mongo-collection)))

(defn- update!- [update-operations-and-mongo-collection upsert?]
  (let [[update-operations mongo-collection] (split-last update-operations-and-mongo-collection)]
    (proper/update! (query/proper-mongo-collection<- mongo-collection)
                    (query/query-parameters mongo-collection)
                    (apply hash-map update-operations)
                    upsert?)))
  
(defmacro ^{:private true} defupdate [name upsert?]
  `(defn ~name [& update-operations-and-mongo-collection#]
     (update!- update-operations-and-mongo-collection# ~upsert?)))

(defupdate update! false)

(defupdate upsert! true)

(defn update-multi! [& update-operations-and-mongo-collection]
  (let [[update-operations mongo-collection] (split-last update-operations-and-mongo-collection)]
    (proper/update-multi! (query/proper-mongo-collection<- mongo-collection)
                          (query/query-parameters mongo-collection)
                          (apply hash-map update-operations))))

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
