(ns mongoika.request)

(def ^{:dynamic true} *db-request-counters* {})

(defn db-request-counters [database]
  (if (*db-request-counters* database)
    *db-request-counters*
    (assoc *db-request-counters* database (mongoika.DBRequestCounter. database))))

(defmacro with-request [database & body]
  `(let [database# ~database]
     (binding [*db-request-counters* (db-request-counters database#)]
       (let [db-request-counter# (*db-request-counters* database#)]
         (.increment db-request-counter#)
         (try (do ~@body)
              (finally (.decrement db-request-counter#)))))))

(defn new-frame [database]
  (if-let [db-request-counter (*db-request-counters* database)]
    (.newFrame db-request-counter)
    nil))
