(ns mongoika.utils
  (require [clojure.string :as string])
  (import [java.io InputStream]))

(defn lazy-seq<-input-stream [in buffer-size]
  (lazy-seq (let [bytes (byte-array buffer-size)
                  read-length (.read ^InputStream in ^bytes bytes)]
              (if (= -1 read-length)
                nil
                (lazy-cat (take read-length bytes)
                          (lazy-seq<-input-stream in buffer-size))))))
