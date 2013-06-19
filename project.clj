(defproject mongoika "0.8.5"
  :description "Clojure MongoDB Library"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.mongodb/mongo-java-driver "2.9.3"]]
  :dev-dependencies [[swank-clojure "1.4.0-SNAPSHOT"]]
  :java-source-paths ["src/java"]
  :aot [mongoika.GridFSDBFileSettable
        mongoika.ProperMongoDBCollectionAdapter
        mongoika.DBRequestCounter
        mongoika.DBCursorWrapper
        mongoika.QuerySequence
        mongoika])
