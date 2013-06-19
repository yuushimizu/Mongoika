(ns mongoika.request)

(def ^{:dynamic true} *db-request-counter* nil)

(defmacro with-request [database & body]
  `(binding [*db-request-counter* (or *db-request-counter* (mongoika.DBRequestCounter. ~database))]
     (.increment *db-request-counter*)
     (try (do ~@body)
          (finally (.decrement *db-request-counter*)))))

(defn new-frame []
  (if *db-request-counter*
    (.newFrame *db-request-counter*)
    nil))
