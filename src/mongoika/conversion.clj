(ns mongoika.conversion
  (require [clojure.string :as string])
  (import [clojure.lang Named Keyword]
          [com.mongodb DBCollection BasicDBObject DBObject BasicDBList]))

(defprotocol StringSource
  (str<- [this]))
(extend-protocol StringSource
  String
  (str<- [this] this)
  Named
  (str<- [this] (name this))
  Object
  (str<- [this] (str this)))

(defprotocol KeywordSource
  (keyword<- [this]))
(extend-protocol KeywordSource
  Keyword
  (keyword<- [this] this)
  Named
  (keyword<- [this] (keyword (name this)))
  Object
  (keyword<- [this] (keyword (str<- this))))

(defprotocol MongoObjectSource
  (mongo-object<- [this]))

(defn mongo-object<-seq [seq]
  (lazy-seq (map mongo-object<- seq)))

(defn mongo-object<-map [map]
  (let [db-obj (BasicDBObject.)]
    (doseq [[key val] map]
      (.put db-obj (str<- key) (mongo-object<- val)))
    db-obj))

(extend-protocol MongoObjectSource
  clojure.lang.Named
  (mongo-object<- [this] (name this))
  clojure.lang.IPersistentMap
  (mongo-object<- [this] (mongo-object<-map this))
  java.util.List
  (mongo-object<- [this] (mongo-object<-seq this))
  Object
  (mongo-object<- [this] this)
  nil
  (mongo-object<- [this] this))

(defprotocol MongoObject
  (<-mongo-object [this]))

(defn- seq<-mongo-object-seq [seq]
  (lazy-seq (map <-mongo-object seq)))

(defn- map<-mongo-object-map [map]
  (reduce (fn [result [key val]]
            (assoc result (keyword<- key) (<-mongo-object val)))
          {}
          map))

(defn <-db-object [db-object]
  (map<-mongo-object-map (map #(vector % (.get ^DBObject db-object %))
                              (.keySet ^DBObject db-object))))

(extend-protocol MongoObject
  java.util.Map
  (<-mongo-object [this] (map<-mongo-object-map this))
  DBObject
  (<-mongo-object [this]
    (<-db-object this))
  java.util.List
  (<-mongo-object [this] (seq<-mongo-object-seq this))
  java.util.Iterator
  (<-mongo-object [this] (seq<-mongo-object-seq (lazy-seq (iterator-seq this))))
  BasicDBList
  (<-mongo-object [this] (seq<-mongo-object-seq this))
  Object
  (<-mongo-object [this] this)
  nil
  (<-mongo-object [this] this))
