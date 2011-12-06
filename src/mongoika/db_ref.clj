(ns mongoika.db-ref
  (use [mongoika
        [conversion :only [MongoObject <-mongo-object]]])
  (import [clojure.lang IDeref IPending]
          [com.mongodb DBRef DB]))

(defn- delay-db-ref [db-ref]
  (let [delay (delay (<-mongo-object (.fetch ^DBRef db-ref)))]
    (proxy [DBRef IDeref IPending] [^DB (.getDB ^DBRef db-ref)
                                    ^String (.getRef ^DBRef db-ref)
                                    ^Object (.getId ^DBRef db-ref)]
      (deref []
        @delay)
      (isRealized []
        (realized? delay)))))

(defn derefable-db-ref [db collection-name id]
  (delay-db-ref (DBRef. ^DB db ^String collection-name ^Object id)))
        
(extend-protocol MongoObject
  DBRef
  (<-mongo-object [this]
    (delay-db-ref this)))
