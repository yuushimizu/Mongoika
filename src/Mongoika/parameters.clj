(ns mongoika.parameters
  (use [mongoika
        [conversion :only [mongo-object<- mongo-object<-map]]])
  (import [java.lang.reflect Field]
          [com.mongodb Bytes]))

(defmulti fix-parameter (fn [parameter val] parameter))

(defmethod fix-parameter :default [parameter val]
  val)

(defmulti restriction-special-key identity)

(defmethod restriction-special-key :default [x] x)

(defmacro def-restriction-special-keys [& {:as before-after-map}]
  `(do ~@(map (fn [[before after]]
                `(defmethod restriction-special-key ~before [_#] ~after))
              before-after-map)))

(def-restriction-special-keys
  < :$lt
  <= :$lte
  > :gt
  >= :gte
  mod :$mod
  type :$type
  not :$not)

(defn- convert-restriction-special-keys [restriction]
  (reduce (fn [converted [key val]]
            (assoc converted
              (restriction-special-key key)
              (if (map? val)
                (convert-restriction-special-keys val)
                val)))
          {}
          restriction))

(defmethod fix-parameter :restrict [parameter restriction]
  (mongo-object<- (or (convert-restriction-special-keys restriction) {})))

(defmethod fix-parameter :project [parameter fields]
  (if fields
    (mongo-object<- (zipmap fields (repeat 1)))
    nil))

(defmethod fix-parameter :order [parameter order]
  (mongo-object<-map order))

(defmethod fix-parameter :skip [parameter skip]
  (int skip))

(defmethod fix-parameter :limit [parameter limit]
  (int limit))

(defmethod fix-parameter :batch-size [parameter batch-size]
  (int batch-size))

(def query-option-map
  (apply hash-map
         (mapcat (fn [[field key]]
                   [(keyword (.toLowerCase ^String key)) (.get ^Field field Bytes)])
                 (filter second
                         (map (fn [field]
                                [field (second (first (re-seq #"^QUERYOPTION_(.+)$" (.getName ^Field field))))])
                              (.getFields Bytes))))))

(defn query-options-number [query-options]
  (reduce bit-or
          0
          (map query-option-map query-options)))

(defmethod fix-parameter :query-options [parameter options]
  ((if (keyword? options) query-option-map query-options-number) options))

(defn fix-parameters [parameters]
  (apply hash-map (mapcat (fn [[parameter val]]
                            [parameter (if (nil? val)
                                         nil
                                         (fix-parameter parameter val))])
                          parameters)))

(defmulti merge-parameter (fn [key current new] key))

(defmethod merge-parameter :restrict [key current new]
  (if (empty? current)
    new
    {:$and [current new]}))

(defmethod merge-parameter :project [key current new]
  (set (if (empty? current)
         new
         (filter current new))))

(defmethod merge-parameter :order [key current new]
  (concat new current))

(defmethod merge-parameter :limit [key current new]
  (if current (min current new) new))

(defmethod merge-parameter :skip [key current new]
  (if current (+ current new) new))

(defmethod merge-parameter :batch-size [key current new]
  new)

(defmethod merge-parameter :query-options [key current new]
  (into (or current #{}) new))

(defmethod merge-parameter :after-map-fn [key current new]
  (if current
    (comp new current)
    new))
