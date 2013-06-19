(ns mongoika.proper-mongodb-collection
  (use [mongoika
        [conversion :only [MongoObject
                           str<-
                           keyword<-
                           <-mongo-object
                           <-db-object
                           mongo-object<-]]
        [params :only [fix-param]]])
  (import [clojure.lang IPersistentMap Sequential Named]
          [java.util List Iterator Map]
          [com.mongodb DBCollection DBObject DBCursor WriteResult MapReduceCommand MapReduceCommand$OutputType]
          [com.mongodb.gridfs GridFS GridFSInputFile GridFSDBFile]))

(defprotocol ProperMongoDBCollection
  (collection-name [this])
  (make-cursor [this ^IPersistentMap params])
  (sequence-from-cursor [this ^DBCursor cursor])
  (call-find-one [this ^IPersistentMap params])
  (count-restricted-documents [this ^IPersistentMap restriction])
  (insert! [this ^IPersistentMap params ^IPersistentMap document])
  (insert-multi! [this ^IPersistentMap params ^Sequential documents])
  (update! [this ^IPersistentMap params ^IPersistentMap operations])
  (update-multi! [this ^IPersistentMap params ^IPersistentMap operations])
  (upsert! [this ^IPersistentMap params ^IPersistentMap operations])
  (upsert-multi! [this ^IPersistentMap params ^IPersistentMap operations])
  (delete-one! [this ^IPersistentMap params])
  (call-delete [this ^IPersistentMap params])
  (map-reduce! [this ^IPersistentMap params ^IPersistentMap options]))

(defn ^{:private true} apply-postapply-to-object [object postapply]
  (if (and postapply object) (first (postapply [object])) object))

(defn make-sequence [proper-mongodb-collection {:keys [limit postapply] :as params} db-request-counter-frame]
  (if (and limit (= 0 (fix-param :limit limit)))
    []
    (let [cursor (mongoika.DBCursorWrapper. (make-cursor proper-mongodb-collection params) db-request-counter-frame)
          cursor-sequence (map <-mongo-object (sequence-from-cursor proper-mongodb-collection cursor))]
      (if postapply (postapply cursor-sequence) cursor-sequence))))

(defn count-documents [proper-mongodb-collection {:keys [skip limit restrict] :as params}]
  (let [count (count-restricted-documents proper-mongodb-collection restrict)
        count (if skip (- count skip) count)
        count (if (neg? count) 0 count)]
    (if limit
      (min limit count)
      count)))

(defn fetch-one [proper-mongodb-collection {:keys [order skip postapply] :as params}]
  (if (or order (and skip (not (= 0 skip))))
    (first (make-sequence proper-mongodb-collection (assoc params :limit 1) nil))
    (apply-postapply-to-object (<-mongo-object (call-find-one proper-mongodb-collection params)) postapply)))

(defn delete! [proper-mongodb-collection {:keys [restrict skip limit] :as params}]
  (when (or skip limit)
    (throw (UnsupportedOperationException. "Deletion with limit or skip is unsupported.")))
  (call-delete proper-mongodb-collection params)
  nil)

(defn ^{:private true} apply-db-cursor-params! [cursor params]
  (doseq [[param apply-fn] {:order #(.sort ^DBCursor cursor ^DBObject %)
                            :skip #(.skip ^DBCursor cursor ^int %)
                            :limit #(.limit ^DBCursor cursor ^int %)
                            :batch-size #(.batchSize ^DBCursor cursor ^int %)
                            :query-options #(.setOptions ^DBCursor cursor ^int %)}]
    (if (contains? params param)
      (apply-fn (fix-param param (param params)))))
  cursor)

(defn ^{:private true} update-in-db-collection [db-collection {:keys [restrict project order skip postapply] :as params} operations upsert?]
  (when skip
    (throw (UnsupportedOperationException. "Update with limit or skip is unsupported.")))
  (let [updated-object (<-mongo-object (.findAndModify ^DBCollection db-collection
                                                       ^DBObject (fix-param :restrict restrict)
                                                       ^DBObject (fix-param :project project)
                                                       ^DBObject (fix-param :order order)
                                                       false ; remove
                                                       ^DBObject (mongo-object<- operations)
                                                       true ; returnNew
                                                       upsert?))]
    (apply-postapply-to-object updated-object postapply)))

(defn ^{:private true} update-multi-in-db-collection [db-collection ^IPersistentMap {:keys [restrict skip limit] :as params} ^IPersistentMap operations upsert?]
  (when skip
    (throw (UnsupportedOperationException. "Update with skip is unsupported.")))
  (if limit
    (if (not (= 1 (fix-param :limit limit)))
      (throw (UnsupportedOperationException. "Update with limit is supported only with 1."))
      (if (update-in-db-collection db-collection params operations upsert?) 1 0))
    (.getN ^WriteResult (.update ^DBCollection db-collection
                                 ^DBObject (fix-param :restrict restrict)
                                 ^DBObject (mongo-object<- operations)
                                 upsert?
                                 true)))) ; multi

(def ^{:private true} map-reduce-command-output-type {:inline MapReduceCommand$OutputType/INLINE
                                                      :merge MapReduceCommand$OutputType/MERGE
                                                      :reduce MapReduceCommand$OutputType/REDUCE
                                                      :replace MapReduceCommand$OutputType/REPLACE})

(extend-type DBCollection
  ProperMongoDBCollection
  (collection-name [this]
    (.getName this))
  (make-cursor [this ^IPersistentMap {:keys [restrict project] :as params}]
    (apply-db-cursor-params! (.find ^DBCollection this
                                    ^DBObject (fix-param :restrict restrict)
                                    ^DBObject (fix-param :project project))
                             params))
  (sequence-from-cursor [this ^DBCursor cursor]
    (iterator-seq cursor))
  (call-find-one [this ^IPersistentMap {:keys [restrict project] :as params}]
    (.findOne ^DBCollection this
              ^DBObject (fix-param :restrict restrict)
              ^DBObject (fix-param :project project)))
  (count-restricted-documents [this ^IPersistentMap restriction]
    (.count ^DBCollection this
            ^DBObject (fix-param :restrict restriction)))
  (insert! [this ^IPersistentMap params ^IPersistentMap document]
    (first (insert-multi! this params [document])))
  (insert-multi! [this ^IPersistentMap {:keys [postapply]} ^Sequential documents]
    (let [mongo-objects (map mongo-object<- documents)]
      (.insert ^DBCollection this
               ^List mongo-objects)
      (let [inserted-objects (map <-mongo-object mongo-objects)]
        (if postapply (postapply inserted-objects) inserted-objects))))
  (call-delete [this ^IPersistent {:keys [restrict]}]
    (.remove ^DBCollection this
             ^DBObject (fix-param :restrict restrict)))
  (update! [this ^IPersistentMap params ^IPersistentMap operations]
    (update-in-db-collection this params operations false))
  (update-multi! [this ^IPersistentMap {:keys [restrict skip limit] :as params} ^IPersistentMap operations]
    (update-multi-in-db-collection this params operations false))
  (upsert! [this ^IPersistentMap params ^IPersistentMap operations]
    (update-in-db-collection this params operations true))
  (upsert-multi! [this ^IPersistentMap params ^IPersistentMap operations]
    (update-multi-in-db-collection this params operations true))
  (delete-one! [this ^IPersistentMap {:keys [restrict project order skip postapply] :as params}]
    (when skip (throw (UnsupportedOperationException. "Deletion with skip is unsupported.")))
    (let [deleted-object (<-mongo-object (.findAndModify ^DBCollection this
                                                         ^DBObject (fix-param :restrict restrict)
                                                         ^DBObject (fix-param :project project)
                                                         ^DBObject (fix-param :order order)
                                                         true ; remove
                                                         nil ; update
                                                         false ; returnNew
                                                         false))] ;upsert
      (apply-postapply-to-object deleted-object postapply)))
  (delete! [this ^IPersistentMap params]
    (delete! this params))
  (map-reduce! [this ^IPersistentMap {:keys [limit order restrict skip postapply] :as params} ^IPersistentMap {:keys [map reduce finalize out out-type scope verbose] :as options}]
    (when skip (throw (UnsupportedOperationException. "Map/Reduce with skip is unsupported.")))
    (when postapply (throw (UnsupportedOperationException. "Map/Reduce with postapply is unsupported.")))
    (<-mongo-object (.mapReduce this
                                ^MapReduceCommand (let [command (MapReduceCommand. ^DBCollection this
                                                                                   ^String map
                                                                                   ^String reduce
                                                                                   ^String (when out (collection-name out))
                                                                                   ^MapReduceCommand$OutputType (or (map-reduce-command-output-type out-type) out-type MapReduceCommand$OutputType/REPLACE)
                                                                                   ^DBObject (fix-param :restrict restrict))]
                                                    (when limit (.setLimit command ^int (fix-param :limit limit)))
                                                    (when order (.setSort command ^DBObject (fix-param :order order)))
                                                    (when finalize (.setFinalize command ^String finalize))
                                                    (when scope (.setScope command ^Map (clojure.core/reduce (fn [variables [key val]]
                                                                                                               (assoc variables ((if (instance? Named key) name str) key) (mongo-object<- val)))
                                                                                                             {}
                                                                                                             scope)))
                                                    (when-not (nil? verbose) (.setVerbose command ^Boolean (boolean verbose)))
                                                    command)))))

(extend-protocol MongoObject
  GridFSDBFile
  (<-mongo-object [this]
    (assoc (<-db-object this)
      :data (.getInputStream ^GridFSDBFile this)))
  GridFSInputFile
  (<-mongo-object [this]
    (<-db-object this)))

(extend-type GridFS
  ProperMongoDBCollection
  (collection-name [this]
    (.getBucketName this))
  (make-cursor [this ^IPersistentMap {:keys [restrict] :as params}]
    (apply-db-cursor-params! (.getFileList ^GridFS this
                                           ^DBObject (fix-param :restrict restrict))
                             params))
  (sequence-from-cursor [this ^DBCursor cursor]
    (map #(mongoika.GridFSDBFileSettable. this %)
         (iterator-seq cursor)))
  (call-find-one [this ^IPersistentMap {:keys [restrict]}]
    (.findOne ^GridFS this
              ^DBObject (fix-param :restrict restrict)))
  (count-restricted-documents [this ^IPersistentMap restriction]
    (.count (.getFileList ^GridFS this
                          ^DBObject (fix-param :restrict restriction))))
  (insert! [this ^IPersistentMap {:keys [postapply]} ^IPersistentMap {:keys [data] :as document}]
    (let [file (.createFile ^GridFS this data)]
      (doseq [[attribute value] (dissoc document :data)]
        (.put ^GridFSInputFile file ^String (str<- attribute) ^Object (mongo-object<- value)))
      ;; Avoid a bug: An invalid length is set if the chunk size is less than or equal to the data length.
      ;; The save method set the chunk size before saving if it received a chunk size.
      (let [chunk-size (.getChunkSize file)]
        (.setChunkSize file GridFS/DEFAULT_CHUNKSIZE)
        (.save file chunk-size))
      (let [inserted-file (assoc (<-mongo-object file)
                            :data data)]
        (apply-postapply-to-object inserted-file postapply))))
  (insert-multi! [this ^IPersistentMap params ^Sequential documents]
    (doall (map #(insert! this params %) documents)))
  (call-delete [this ^IPersistentMap {:keys [restrict]}]
    (.remove ^GridFS this
             ^DBObject (fix-param :restrict restrict)))
  (update! [this ^IPersistentMap params ^IPersistentMap operations]
    (throw (UnsupportedOperationException. "GridFS does not support update!.")))
  (update-multi! [this ^IPersistentMap params ^IPersistentMap operations]
    (throw (UnsupportedOperationException. "GridFS does not support update!.")))
  (upsert! [this ^IPersistentMap params ^IPersistentMap operations]
    (throw (UnsupportedOperationException. "GridFS does not support upsert!.")))
  (delete-one! [this ^IPersistentMap params]
    (throw (UnsupportedOperationException. "GridFS does not support delete-one!.")))
  (delete! [this ^IPersistentMap params]
    (delete! this params))
  (map-reduce! [this ^IPersistentMap params ^IPersistentMap options]
    (throw (UnsupportedOperationException. "GridFS does not support map-reduce!."))))
