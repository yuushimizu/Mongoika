(ns mongoika.query
  (import [clojure.lang LazySeq IObj Counted IPersistentMap ISeq Sequential IPersistentCollection IPending]
          [java.util List Collection Iterator ListIterator]))

(defprotocol QuerySource
  (collection-name [this])
  (make-seq [this ^IPersistentMap params])
  (count-docs [this ^IPersistentMap params])
  (fetch-one [this ^IPersistentMap params])
  (insert! [this ^IPersistentMap params ^IPersistentMap doc])
  (insert-multi! [this ^IPersistentMap params ^Sequential docs])
  (update! [this ^IPersistentMap params ^IPersistentMap operations])
  (update-multi! [this ^IPersistentMap params ^IPersistentMap operations])
  (upsert! [this ^IPersistentMap params ^IPersistentMap operations])
  (delete-one! [this ^IPersistentMap params])
  (delete! [this ^IPersistentMap params])
  (map-reduce! [this ^IPersistentMap params ^IPersistentMap options]))

(defprotocol QuerySourceWithAdditionalParams
  (additional-params [this]))

(extend-protocol QuerySourceWithAdditionalParams
  Object
  (additional-params [this] {}))

(declare make-query)

(deftype query [^IPersistentMap meta
                ^LazySeq docsLazySeq
                querySource
                ^IPersistentMap params]
  Object
  (^int hashCode [^query this]
    (.hashCode docsLazySeq))
  (^boolean equals [^query this ^Object o]
    (.equals docsLazySeq o))
  IObj
  (^IPersistentMap meta [^query this]
    meta)
  (^IObj withMeta [^query this ^IPersistentMap meta]
    (make-query querySource params meta))
  Counted
  (^int count [^query this]
    (if (realized? docsLazySeq)
      (count docsLazySeq)
      (count-docs querySource params)))
  Sequential
  ISeq
  (^ISeq seq [^query this]
    (.seq docsLazySeq))
  (^IPersistentCollection empty [^query this]
    (.empty docsLazySeq))
  (^boolean equiv [^query this ^Object o]
    (.equiv docsLazySeq o))
  (^Object first [^query this]
    (.first docsLazySeq))
  (^ISeq next [^query this]
    (.next docsLazySeq))
  (^ISeq more [^query this]
    (.more docsLazySeq))
  (^ISeq cons [^query this ^Object o]
    (.cons docsLazySeq o))
  List
  (^objects toArray [^query this]
    (.toArray docsLazySeq))
  (^objects toArray [^query this ^objects a]
    (.toArray docsLazySeq a))
  (^boolean remove [^query this ^Object o]
    (.remove docsLazySeq o))
  (^void clear [^query this]
    (.clear docsLazySeq))
  (^boolean retainAll [^query this ^Collection c]
    (.retainAll docsLazySeq c))
  (^boolean removeAll [^query this ^Collection c]
    (.removeAll docsLazySeq c))
  (^boolean containsAll [^query this ^Collection c]
    (.containsAll docsLazySeq c))
  (^int size [^query this]
    (.size docsLazySeq))
  (^boolean isEmpty [^query this]
    (.isEmpty docsLazySeq))
  (^boolean contains [^query this ^Object o]
    (.contains docsLazySeq o))
  (^Iterator iterator [^query this]
    (.iterator docsLazySeq))
  (^List subList [^query this ^int from ^int to]
    (.subList docsLazySeq from to))
  (^Object set [^query this ^int i ^Object o]
    (.set docsLazySeq i o))
  (^int indexOf [^query this ^Object o]
    (.indexOf docsLazySeq o))
  (^int lastIndexOf [^query this ^Object o]
    (.lastIndexOf docsLazySeq o))
  (^ListIterator listIterator [^query this]
    (.listIterator docsLazySeq))
  (^ListIterator listIterator [^query this ^int i]
    (.listIterator docsLazySeq i))
  (^Object get [^query this ^int i]
    (.get docsLazySeq i))
  (^boolean add [^query this ^Object o]
    (.add docsLazySeq o))
  (^void add [^query this ^int i ^Object o]
    (.add docsLazySeq i o))
  (^boolean addAll [^query this ^int i ^Collection c]
    (.addAll docsLazySeq i c))
  (^boolean addAll [^query this ^Collection c]
    (.addAll docsLazySeq c))
  IPending
  (^boolean isRealized [^query this]
    (.isRealized docsLazySeq)))

(defn make-query
  ([query-source params meta]
     (query. ^IPersistentMap meta ^LazySeq (lazy-seq (make-seq query-source params)) query-source ^IPersistentMap params))
  ([query-source params]
     (make-query query-source params (if (instance? clojure.lang.IMeta query-source) (meta query-source) {})))
  ([query-source]
     (make-query query-source {})))

(defmulti merge-param (fn [key current new] key))

(defmethod merge-param :restrict [key current new]
  (if (empty? current)
    new
    {:$and [current new]}))

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

(defmethod merge-param :map-after [key current new]
  (if current
    (comp new current)
    new))

(defn merge-params [current new]
  (reduce (fn [merged [key new-value]] (assoc merged key (merge-param key (get merged key) new-value)))
          current
          new))

(extend-type query
  QuerySource
  (collection-name [this]
    (collection-name (.querySource this)))
  (make-seq [this ^IPersistentMap params]
    (make-seq (.querySource this) (merge-params (additional-params this) params)))
  (count-docs [this ^IPersistentMap params]
    (count-docs (.querySource this) (merge-params (additional-params this) params)))
  (fetch-one [this ^IPersistentMap params]
    (fetch-one (.querySource this) (merge-params (additional-params this) params)))
  (insert! [this ^IPersistentMap params ^IPersistentMap doc]
    (insert! (.querySource this) (merge-params (additional-params this) params) doc))
  (insert-multi! [this ^IPersistentMap params ^Sequential docs]
    (insert-multi! (.querySource this) (merge-params (additional-params this) params) docs))
  (update! [this ^IPersistentMap params ^IPersistentMap operations]
    (update! (.querySource this) (merge-params (additional-params this) params) operations))
  (update-multi! [this ^IPersistentMap params ^IPersistentMap operations]
    (update-multi! (.querySource this) (merge-params (additional-params this) params) operations))
  (upsert! [this ^IPersistentMap params ^IPersistentMap operations]
    (upsert! (.querySource this) (merge-params (additional-params this) params) operations))
  (delete-one! [this ^IPersistentMap params]
    (delete-one! (.querySource this) (merge-params (additional-params this) params)))
  (delete! [this ^IPersistentMap params]
    (delete! (.querySource this) (merge-params (additional-params this) params)))
  (map-reduce! [this ^IPersistentMap params ^IPersistentMap options]
    (map-reduce! (.querySource this) (merge-params (additional-params this) params) options))
  QuerySourceWithAdditionalParams
  (additional-params [this]
    (.params this)))

(defn add-param [query-source key value]
  (make-query query-source {key value}))
