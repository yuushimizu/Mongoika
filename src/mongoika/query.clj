(ns mongoika.query
  (use [mongoika
        [conversion :only [<-mongo-object mongo-object<-]]
        [parameters :only [merge-parameter]]])
  (require [mongoika
            [proper-mongo-collection :as proper]])
  (import [clojure.lang LazySeq IObj Counted IPersistentMap ISeq Sequential IPersistentCollection IPending]
          [com.mongodb DBCollection DBObject DBCursor WriteResult]
          [com.mongodb.gridfs GridFS]
          [java.util List Collection Iterator ListIterator]))

(declare new-query)

(deftype query [^IPersistentMap meta
                ^LazySeq cursorLazySeq
                properMongoCollection
                ^IPersistentMap parameters]
  Object
  (^int hashCode [^query this]
    (.hashCode cursorLazySeq))
  (^boolean equals [^query this ^Object o]
    (.equals cursorLazySeq o))
  IObj
  (^IPersistentMap meta [^query this]
    meta)
  (^IObj withMeta [^query this ^IPersistentMap meta]
    (new-query properMongoCollection parameters meta))
  Counted
  (^int count [^query this]
    (if (realized? cursorLazySeq)
      (count cursorLazySeq)
      (proper/get-count properMongoCollection parameters)))
  Sequential
  ISeq
  (^ISeq seq [^query this]
    (.seq cursorLazySeq))
  (^IPersistentCollection empty [^query this]
    (.empty cursorLazySeq))
  (^boolean equiv [^query this ^Object o]
    (.equiv cursorLazySeq o))
  (^Object first [^query this]
    (.first cursorLazySeq))
  (^ISeq next [^query this]
    (.next cursorLazySeq))
  (^ISeq more [^query this]
    (.more cursorLazySeq))
  (^ISeq cons [^query this ^Object o]
    (.cons cursorLazySeq o))
  List
  (^objects toArray [^query this]
    (.toArray cursorLazySeq))
  (^objects toArray [^query this ^objects a]
    (.toArray cursorLazySeq a))
  (^boolean remove [^query this ^Object o]
    (.remove cursorLazySeq o))
  (^void clear [^query this]
    (.clear cursorLazySeq))
  (^boolean retainAll [^query this ^Collection c]
    (.retainAll cursorLazySeq c))
  (^boolean removeAll [^query this ^Collection c]
    (.removeAll cursorLazySeq c))
  (^boolean containsAll [^query this ^Collection c]
    (.containsAll cursorLazySeq c))
  (^int size [^query this]
    (.size cursorLazySeq))
  (^boolean isEmpty [^query this]
    (.isEmpty cursorLazySeq))
  (^boolean contains [^query this ^Object o]
    (.contains cursorLazySeq o))
  (^Iterator iterator [^query this]
    (.iterator cursorLazySeq))
  (^List subList [^query this ^int from ^int to]
    (.subList cursorLazySeq from to))
  (^Object set [^query this ^int i ^Object o]
    (.set cursorLazySeq i o))
  (^int indexOf [^query this ^Object o]
    (.indexOf cursorLazySeq o))
  (^int lastIndexOf [^query this ^Object o]
    (.lastIndexOf cursorLazySeq o))
  (^ListIterator listIterator [^query this]
    (.listIterator cursorLazySeq))
  (^ListIterator listIterator [^query this ^int i]
    (.listIterator cursorLazySeq i))
  (^Object get [^query this ^int i]
    (.get cursorLazySeq i))
  (^boolean add [^query this ^Object o]
    (.add cursorLazySeq o))
  (^void add [^query this ^int i ^Object o]
    (.add cursorLazySeq i o))
  (^boolean addAll [^query this ^int i ^Collection c]
    (.addAll cursorLazySeq i c))
  (^boolean addAll [^query this ^Collection c]
    (.addAll cursorLazySeq c))
  IPending
  (^boolean isRealized [^query this]
    (.isRealized cursorLazySeq)))

(defprotocol MongoCollection
  (proper-mongo-collection<- [this])
  (query-parameters [this]))
(extend-protocol MongoCollection
  query
  (proper-mongo-collection<- [this]
    (.properMongoCollection ^query this))
  (query-parameters [this]
    (.parameters ^query this))
  Object
  (proper-mongo-collection<- [this] this)
  (query-parameters [this]
    {}))

(defn- new-query
  ([mongo-collection parameters meta]
     (let [proper-mongo-collection (proper-mongo-collection<- mongo-collection)]
       (query. ^IPersistentMap meta
               ^LazySeq (lazy-seq (proper/fetch proper-mongo-collection parameters))
               proper-mongo-collection
               ^IPersistentMap parameters)))
  ([mongo-collection parameters]
     (new-query mongo-collection parameters (if (instance? clojure.lang.IMeta mongo-collection) (meta mongo-collection) {}))))

(defn create [mongo-collection]
  (new-query mongo-collection (query-parameters mongo-collection)))

(defn assoc-parameter [mongo-collection key value]
  (new-query mongo-collection (assoc (query-parameters mongo-collection) key value)))

(defn add-parameter [mongo-collection key new-value]
  (assoc-parameter mongo-collection key (merge-parameter key (get (query-parameters mongo-collection) key) new-value)))
