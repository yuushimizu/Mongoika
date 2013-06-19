(ns mongoika.query
  (require [mongoika
            [proper-mongodb-collection :as proper-mongodb-collection]])
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
  (parameters [this] {}))

(declare make-query)

(deftype query [^IPersistentMap meta
                ^LazySeq documentsLazySequence
                properMongoDBCollection
                ^IPersistentMap parameters]
  Object
  (^int hashCode [^query this]
    (.hashCode documentsLazySequence))
  (^boolean equals [^query this ^Object o]
    (.equals documentsLazySequence o))
  IObj
  (^IPersistentMap meta [^query this]
    meta)
  (^IObj withMeta [^query this ^IPersistentMap meta]
    (make-query properMongoDBCollection parameters meta))
  Counted
  (^int count [^query this]
    (if (realized? documentsLazySequence)
      (count documentsLazySequence)
      (proper-mongodb-collection/count-documents properMongoDBCollection parameters)))
  Sequential
  ISeq
  (^ISeq seq [^query this]
    (.seq documentsLazySequence))
  (^IPersistentCollection empty [^query this]
    (.empty documentsLazySequence))
  (^boolean equiv [^query this ^Object o]
    (.equiv documentsLazySequence o))
  (^Object first [^query this]
    (.first documentsLazySequence))
  (^ISeq next [^query this]
    (.next documentsLazySequence))
  (^ISeq more [^query this]
    (.more documentsLazySequence))
  (^ISeq cons [^query this ^Object o]
    (.cons documentsLazySequence o))
  List
  (^objects toArray [^query this]
    (.toArray documentsLazySequence))
  (^objects toArray [^query this ^objects a]
    (.toArray documentsLazySequence a))
  (^boolean remove [^query this ^Object o]
    (.remove documentsLazySequence o))
  (^void clear [^query this]
    (.clear documentsLazySequence))
  (^boolean retainAll [^query this ^Collection c]
    (.retainAll documentsLazySequence c))
  (^boolean removeAll [^query this ^Collection c]
    (.removeAll documentsLazySequence c))
  (^boolean containsAll [^query this ^Collection c]
    (.containsAll documentsLazySequence c))
  (^int size [^query this]
    (.size documentsLazySequence))
  (^boolean isEmpty [^query this]
    (.isEmpty documentsLazySequence))
  (^boolean contains [^query this ^Object o]
    (.contains documentsLazySequence o))
  (^Iterator iterator [^query this]
    (.iterator documentsLazySequence))
  (^List subList [^query this ^int from ^int to]
    (.subList documentsLazySequence from to))
  (^Object set [^query this ^int i ^Object o]
    (.set documentsLazySequence i o))
  (^int indexOf [^query this ^Object o]
    (.indexOf documentsLazySequence o))
  (^int lastIndexOf [^query this ^Object o]
    (.lastIndexOf documentsLazySequence o))
  (^ListIterator listIterator [^query this]
    (.listIterator documentsLazySequence))
  (^ListIterator listIterator [^query this ^int i]
    (.listIterator documentsLazySequence i))
  (^Object get [^query this ^int i]
    (.get documentsLazySequence i))
  (^boolean add [^query this ^Object o]
    (.add documentsLazySequence o))
  (^void add [^query this ^int i ^Object o]
    (.add documentsLazySequence i o))
  (^boolean addAll [^query this ^int i ^Collection c]
    (.addAll documentsLazySequence i c))
  (^boolean addAll [^query this ^Collection c]
    (.addAll documentsLazySequence c))
  IPending
  (^boolean isRealized [^query this]
    (.isRealized documentsLazySequence)))

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

(extend-type query
  QuerySource
  (proper-mongodb-collection [this]
    (.properMongoDBCollection this))
  (parameters [this]
    (.parameters this)))

(defn make-query
  ([query-source params meta]
     (let [proper-mongodb-collection (proper-mongodb-collection query-source)
           merged-params (merge-params (parameters query-source) params)]
       (query. ^IPersistentMap meta ^LazySeq (lazy-seq (proper-mongodb-collection/make-sequence proper-mongodb-collection merged-params)) proper-mongodb-collection ^IPersistentMap merged-params)))
  ([query-source params]
     (make-query query-source params (if (instance? IMeta query-source) (meta query-source) {})))
  ([query-source]
     (make-query query-source {})))

(defn add-param [query-source key value]
  (make-query query-source {key value}))
