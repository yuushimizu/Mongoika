(ns mongoika.proper-mongo-collection
  (use [mongoika
        [conversion :only [MongoObject
                           str<-
                           keyword<-
                           <-mongo-object
                           <-db-object
                           mongo-object<-]]
        [parameters :only [fix-parameter]]])
  (import [clojure.lang IPersistentMap Sequential]
          [java.util List Iterator]
          [com.mongodb DBCollection DBObject DBCursor WriteResult]
          [com.mongodb.gridfs GridFS GridFSInputFile GridFSDBFile]))

(defprotocol ProperMongoCollection
  (make-cursor-seq [this ^IPersistentMap parameters])
  (call-find-one-method [this ^IPersistentMap parameters])
  (call-delete-method [this ^IPersistentMap parameters])
  (restricted-count [this ^IPersistentMap restriction])
  (insert! [this ^IPersistentMap parameters ^IPersistentMap obj])
  (insert-multi! [this ^IPersistentMap parameters ^Sequential objs])
  (update! [this ^IPersistentMap parameters ^IPersistentMap update-operations ^Boolean upsert?])
  (update-multi! [this ^IPersistentMap parameters ^IPersistentMap update-operations]))

(defn- apply-db-cursor-parameters! [cursor parameters]
  (doseq [[parameter apply-fn] {:order #(.sort ^DBCursor cursor ^DBObject %)
                                :skip #(.skip ^DBCursor cursor ^int %)
                                :limit #(.limit ^DBCursor cursor ^int %)
                                :batch-size #(.batchSize ^DBCursor cursor ^int %)
                                :query-options #(.setOptions ^DBCursor cursor ^int %)}]
    (if (contains? parameters parameter)
      (apply-fn (fix-parameter parameter (parameter parameters)))))
  cursor)

(extend-type DBCollection
  ProperMongoCollection
  (make-cursor-seq [this ^IPersistentMap {:keys [restrict project] :as parameters}]
    (map <-mongo-object
         (iterator-seq (apply-db-cursor-parameters! (.find ^DBCollection this
                                                           ^DBObject (fix-parameter :restrict restrict)
                                                           ^DBObject (fix-parameter :project project))
                                                    parameters))))
  (call-find-one-method [this ^IPersistentMap {:keys [restrict project]}]
    (.findOne ^DBCollection this
              ^DBObject (fix-parameter :restrict restrict)
              ^DBObject (fix-parameter :project project)))
  (call-delete-method [this ^IPersistent {:keys [restrict]}]
    (.remove ^DBCollection this
             ^DBObject (fix-parameter :restrict restrict)))
  (restricted-count [this ^IPersistentMap restriction]
    (.count ^DBCollection this
            ^DBObject (fix-parameter :restrict restriction)))
  (insert! [this ^IPersistentMap parameters ^IPersistentMap obj]
    (first (insert-multi! this parameters [obj])))
  (insert-multi! [this ^IPersistentMap {:keys [after-map-fn]} ^Sequential objs]
    (let [mongo-objects (map mongo-object<- objs)]
      (.insert ^DBCollection this
               ^List mongo-objects)
      (let [inserted-objects (map <-mongo-object mongo-objects)]
        (if after-map-fn
          (map after-map-fn inserted-objects)
          inserted-objects))))
  (update! [this ^IPersistentMap {:keys [restrict project order skip after-map-fn]} ^DBObject update-operations ^Boolean upsert?]
    (when skip
      (throw (UnsupportedOperationException. "Update with limit or skip is unsupported.")))
    (let [updated-object (<-mongo-object (.findAndModify ^DBCollection this
                                                         ^DBObject (fix-parameter :restrict restrict)
                                                         ^DBObject (fix-parameter :project project)
                                                         ^DBObject (fix-parameter :order order)
                                                         false ; remove
                                                         ^DBObject (mongo-object<- update-operations)
                                                         true ; returnNew
                                                         upsert?))]
      (if after-map-fn
        (after-map-fn updated-object)
        updated-object)))
  (update-multi! [this ^IPersistentMap {:keys [restrict skip limit] :as parameters} ^DBObject update-operations]
    (when skip
      (throw (UnsupportedOperationException. "Update with skip is unsupported.")))
    (if limit
      (if (not (= 1 (fix-parameter :limit limit)))
        (throw (UnsupportedOperationException. "Update with limit is supported only with 1."))
        (if (update! this parameters update-operations false) 1 0))
      (.getN ^WriteResult (.update ^DBCollection this
                                   ^DBObject (fix-parameter :restrict restrict)
                                   ^DBObject (mongo-object<- update-operations)
                                   false ; upsert
                                   true))))) ; multi

(extend-protocol MongoObject
  GridFSDBFile
  (<-mongo-object [this]
    (assoc (<-db-object this)
      :data (.getInputStream ^GridFSDBFile this)))
  GridFSInputFile
  (<-mongo-object [this]
    (<-db-object this)))

(gen-interface :name mongoika.proper-mongo-collection.GridFSDBFileSettable
               :methods [[_setFrom [com.mongodb.gridfs.GridFS com.mongodb.gridfs.GridFSDBFile] com.mongodb.gridfs.GridFSDBFile]])

(extend-type GridFS
  ProperMongoCollection
  (make-cursor-seq [this ^IPersistentMap {:keys [restrict] :as parameters}]
    (map (fn [file]
           ;; GridFS#getFileList does not set a GridFS in fetched files.
           ;; A GridFS mus be set in a GridFSDBFile when read data from it.
           (<-mongo-object (._setFrom (proxy [GridFSDBFile mongoika.proper-mongo-collection.GridFSDBFileSettable] []
                                        (_setFrom [^GridFS grid-fs ^GridFSDBFile file]
                                          ;; GridFSDBFile does not support putAll().
                                          (doseq [key (.keySet ^DBObject file)]
                                            (.put this
                                                  ^String key
                                                  ^Object (.get ^DBObject file ^String key)))
                                          (.setGridFS this ^GridFS grid-fs)
                                          this))
                                      this
                                      file)))
         (iterator-seq (apply-db-cursor-parameters! (.getFileList ^GridFS this
                                                                  ^DBObject (fix-parameter :restrict restrict))
                                                    parameters))))
  (call-find-one-method [this ^IPersistentMap {:keys [restrict]}]
    (.findOne ^GridFS this
              ^DBObject (fix-parameter :restrict restrict)))
  (call-delete-method [this ^IPersistentMap {:keys [restrict]}]
    (.remove ^GridFS this
             ^DBObject (fix-parameter :restrict restrict)))
  (restricted-count [this ^IPersistentMap restriction]
    (.count (.getFileList ^GridFS this
                          ^DBObject (fix-parameter :restrict restriction))))
  (insert! [this ^IPersistentMap {:keys [after-map-fn]} ^IPersistentMap {:keys [data] :as obj}]
    (let [file (.createFile ^GridFS this data)]
      (doseq [[attribute value] (dissoc obj :data)]
        (.put ^GridFSInputFile file ^String (str<- attribute) ^Object (mongo-object<- value)))
      ;; Avoid a bug: An invalid length is set if the chunk size is less than or equal to the data length.
      ;; The save method set the chunk size before saving if it received a chunk size.
      (let [chunk-size (.getChunkSize file)]
        (.setChunkSize file GridFS/DEFAULT_CHUNKSIZE)
        (.save file chunk-size))
      (let [inserted-file (assoc (<-mongo-object file)
                            :data data)]
        (if after-map-fn
          (after-map-fn inserted-file)
          inserted-file))))
  (insert-multi! [this ^IPersistentMap parameters ^Sequential objs]
    (doall (map #(insert! this parameters %) objs)))
  (update! [this ^IPersistentMap parameters ^IPersistentMap update-operations ^Boolean upsert?]
    (throw (UnsupportedOperationException. "GridFS does not support update.")))
  (update-multi! [this ^IPersistentMap parameters ^IPersistentMap update-operations]
    (throw (UnsupportedOperationException. "GridFS does not support update."))))

(defn fetch [proper-mongo-collection ^IPersistentMap {:keys [after-map-fn] :as parameters}]
  (let [cursor-seq (make-cursor-seq proper-mongo-collection parameters)]
    (if after-map-fn
      (map after-map-fn cursor-seq)
      cursor-seq)))

(defn get-count [proper-mongo-collection ^IPersistentMap {:keys [skip limit restrict] :as parameters}]
  (let [count (restricted-count proper-mongo-collection restrict)
        count (if skip (- count skip) count)
        count (if (neg? count) 0 count)]
    (if limit
      (min limit count)
      count)))

(defn fetch-one [proper-mongo-collection ^IPersistentMap {:keys [order skip after-map-fn] :as parameters}]
  (if (or order (and skip (not (= 0 skip))))
    (first (fetch proper-mongo-collection (assoc parameters :limit 1)))
    (let [object (<-mongo-object (call-find-one-method proper-mongo-collection parameters))]
      (if after-map-fn
        (after-map-fn object)
        object))))

(defn delete! [proper-mongo-collection ^IPersistentMap {:keys [restrict skip limit] :as parameters}]
  (when (or skip limit)
    (throw (UnsupportedOperationException. "Deletion with limit or skip is unsupported.")))
  (call-delete-method proper-mongo-collection parameters)
  nil)
