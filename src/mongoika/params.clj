(ns mongoika.params
  (use [mongoika
        [conversion :only [keyword<- mongo-object<- mongo-object<-map]]])
  (import [java.lang.reflect Field]
          [com.mongodb Bytes]))

(defmulti fix-param (fn [param val] param))

(defmethod fix-param :default [param val]
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
  > :$gt
  >= :$gte
  mod :$mod
  type :$type
  not :$not)

(defn- convert-restriction-special-keys [restriction]
  (letfn [(convert-value [val]
            (cond (map? val) (reduce (fn [converted [key val]]
                                       (assoc converted (restriction-special-key key) (convert-value val)))
                                     {}
                                     val)
                  (coll? val) (map convert-value val)
                  :else val))]
    (map (fn [[key val]] [(restriction-special-key key) (convert-value val)])
         restriction)))

(defmethod fix-param :restrict [param restriction]
  (mongo-object<-map (or (convert-restriction-special-keys restriction) {})))

(defmethod fix-param :project [param projection]
  (if projection
    (let [{including-fields true excluding-fields false} projection]
      (mongo-object<- (cond (nil? including-fields) (zipmap excluding-fields (repeat 0))
                            (empty? including-fields) {:_id 1}
                            :else (zipmap (if excluding-fields
                                            (remove excluding-fields including-fields)
                                            including-fields)
                                          (repeat 1)))))
    nil))

(defn- fix-order-value [order]
  (if (and (map? order) (:reverse order))
    (if (let [source (fix-order-value (:reverse order))]
          (and (number? source) (neg? source)))
      1
      -1)
    (cond (= :asc order) 1
          (= :desc order) -1
          (number? order) order
          :else 1)))

(defmethod fix-param :order [param order]
  (mongo-object<-map (map (fn [[field order]] [field (fix-order-value order)]) order)))

(defmethod fix-param :skip [param skip]
  (int skip))

(defmethod fix-param :limit [param limit]
  (int limit))

(defmethod fix-param :batch-size [param batch-size]
  (int batch-size))

(def query-option-map
  (apply hash-map
         (mapcat (fn [[field key]]
                   [(keyword (.toLowerCase ^String key)) (.get ^Field field Bytes)])
                 (filter second
                         (map (fn [field]
                                [field (second (first (re-seq #"^QUERYOPTION_(.+)$" (.getName ^Field field))))])
                              (.getFields Bytes))))))

(defn fix-query-option-value [query-option]
  (if (keyword? query-option) (query-option-map query-option) query-option))

(defn query-options-number [query-options]
  (reduce bit-or
          0
          (map fix-query-option-value query-options)))

(defmethod fix-param :query-options [param options]
  ((if (keyword? options) fix-query-option-value query-options-number) options))

(defn fix-params [params]
  (apply hash-map (mapcat (fn [[param val]]
                            [param (if (nil? val)
                                     nil
                                     (fix-param param val))])
                          params)))
