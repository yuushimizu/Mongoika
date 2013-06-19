(ns mongoika.query
  (require [mongoika
            [proper-mongodb-collection :as proper-mongodb-collection]
            [request :as request]])
  (import [clojure.lang LazySeq IObj Counted IPersistentMap ISeq Sequential IPersistentCollection IPending IMeta]
          [java.util List Collection Iterator ListIterator]
          [com.mongodb DBCollection]
          [com.mongodb.gridfs GridFS]))

(defprotocol QuerySource
  (proper-mongodb-collection [this])
  (parameters [this]))

(extend-protocol QuerySource
  DBCollection
  (proper-mongodb-collection [this] this)
  (parameters [this] {})
  GridFS
  (proper-mongodb-collection [this] this)
  (parameters [this] {})
  mongoika.QuerySequence
  (proper-mongodb-collection [this]
    (.properMongoDBCollection this))
  (parameters [this]
    (.parameters this)))

(defmulti merge-param (fn [key current new] key))

(defmulti unnestable-special-restriction-key? identity)

(def unnestable-special-restriction-keys #{:$explain :$hint :$isolated :$orderby :$showDiskLoc :$snapshot :$query})

(defmethod unnestable-special-restriction-key? :default [key] (boolean (unnestable-special-restriction-keys key)))

(defn merge-restriction-param [current [key value]]
  (cond (empty? current) {key value}
        (unnestable-special-restriction-key? key) (assoc current key value)
        :else (let [{unnestables true nestables false} (group-by #(unnestable-special-restriction-key? (first %)) current)]
                (merge (into {} unnestables)
                       {:$and [(into {} nestables) {key value}]}))))

(defmethod merge-param :restrict [key current new]
  (reduce merge-restriction-param current new))

(defmethod merge-param :project [key current new]
  (if current
    {true (if (get new true)
            (if (get current true)
              (set (filter (get current true) (get new true)))
              (get new true))
            (get current true))
     false (if (get new false)
             (if (get current false)
               (into (get current false) (get new false))
               (get new false))
             (get current false))}
    new))

(def order-reverse `order-reverse)

(defmethod merge-param :order [key current new]
  (if (identical? new order-reverse)
    (if (empty? current)
      [[:$natural :desc]]
      (map (fn [[field order]] [field {:reverse order}]) current))
    (concat new
            (remove (fn [[field order]]
                      (some #(= field (first %)) new))
                    current))))

(defmethod merge-param :limit [key current new]
  (if current (min current new) new))

(defmethod merge-param :skip [key current new]
  (if current (+ current new) new))

(defmethod merge-param :batch-size [key current new]
  new)

(defmethod merge-param :query-options [key current new]
  (into (or current #{}) new))

(defmethod merge-param :postapply [key current new]
  (if current
    (comp new current)
    new))

(defn merge-params [current new]
  (reduce (fn [merged [key new-value]] (assoc merged key (merge-param key (get merged key) new-value)))
          current
          new))

(def proper-mongodb-collection-adapter
  (proxy [mongoika.ProperMongoDBCollectionAdapter] []
    (makeDocumentsLazySequence [proper-mongodb-collection parameters]
      (let [db-request-counter-frame (request/new-frame)]
        (lazy-seq (proper-mongodb-collection/make-sequence proper-mongodb-collection parameters db-request-counter-frame))))
    (countDocuments [proper-mongodb-collection parameters]
      (proper-mongodb-collection/count-documents proper-mongodb-collection parameters))))

(defn make-query
  ([query-source params meta]
     (let [proper-mongodb-collection (proper-mongodb-collection query-source)
           merged-params (merge-params (parameters query-source) params)]
       (mongoika.QuerySequence. ^IPersistentMap meta ^Object proper-mongodb-collection ^IPersistentMap merged-params ^mongoika.ProperMongoDBCollectionAdapter proper-mongodb-collection-adapter)))
  ([query-source params]
     (make-query query-source params (if (instance? IMeta query-source) (meta query-source) {})))
  ([query-source]
     (make-query query-source {})))

(defn add-param [query-source key value]
  (make-query query-source {key value}))
