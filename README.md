![][LogoImage]

# Mongoika

A MongoDB Library for Clojure.

Mongoika simplify building queries behaved like lazy sequences, and supports basic operations, MapReduce and GridFS using Mongo Java Driver.

## Examples

```clojure
;; Use mongoika namespace.
(use 'mongoika)
    
;; Connect to a MongoDB server.
(with-mongo [connection {:host your-mongodb-host :port your-mongodb-port}]
  ;; Bind a database dynamically.
  (with-db-binding (database connection :your-database)
    ;; Index:
    (ensure-index! :fruits {:name :asc} :unique true)
    (ensure-index! :fruits [:price :asc :name :asc])

    ;; Insertion:
    (insert! :fruits {:name "Banana" :color :yellow :price 100})
    (insert! :fruits {:name "Apple" :color :red :price 80})
    (insert! :fruits {:name "Lemon" :color :yellow :price 50})

    ;; Fetch all:
    (query :fruits)
    ; => [{:_id #<ObjectId> :name "Banana" :color "yellow" :price 100}
    ;     {:_id #<ObjectId> :name "Apple" :color "red" :price 80}
    ;     {:_id #<ObjectId> :name "Lemon" :color "yellow" :price 50}]

    ;; Find:
    (restrict :color :yellow :fruits)
    ; => [banana lemon]
    
    (restrict :price {< 100} :fruits)
    ; => [apple lemon]

    ;; Sort:
    (order :price :asc :fruits)
    ; => [lemon apple banana]

    ;; Find and sort:
    (order :price :asc (restrict :color :yellow :fruits))
    ; => [lemon banana]

    ;; Fetch first:
    (fetch-one (order :price :desc (restrict :color :yellow :fruits)))
    ; => banana

    ;; MapReduce:
    (map-reduce! :map "function() {
                         emit(this.color, this.price);
                       }"
                 :reduce "function(key, vals) {
                            var sum = 0;
                            for (var i = 0; i < vals.length; ++i) sum += vals[i];
                            return sum;
                          }"
                 :out :sum-of-prices
                 :fruits)
    (order :color :asc :sum-of-prices)
    ; => [{:_id "red" :value 80.0}
    ;     {:_id "yellow" :value 150.0}]
    ))

```
## Usage
### Connect to a MongoDB server.

`mongo` returns a Mongo instance of Mongo Java Driver. It has a connection pool.

```clojure
(mongo {:host "127.0.0.1" :port 27017})
```

```clojure
; Connect with options
(mongo {:host "127.0.0.1" :port 27017} {:safe true :socketTimeout 5})
```

`with-mongo` binds a Mongo instance to the specified symbol. The mongo instance is closed automatically.

```clojure
(with-mongo [connection {:host "127.0.0.1" :port 27017} {:safe true}]
  ...)
```

### Use a database

`database` returns a DB instance.

```clojure
(database connection :your-database)
```
`with-db-binding` binds a specified database to the dynamic var `*db*`, and most functions in Mongoika use it.

```clojure
(with-db-binding (database connection :your-database)
  ...)
```

You can use `set-default-db!` to set a database to `*db*`.

```clojure
(set-default-db! (database connection :your-database)
```

`bound-db` returns the current bound database.

```clojure
(bound-db)
```

### Insertion

`insert!` inserts a document to the specified collection, and returns the inserted document as a map that has an `_id` field. Each key of a map returned from `insert!` is converted to a keyword.

```clojure
(insert! :foods {:name "Cheese" :quantity 120 :price 300})
; => db.foods.insert({name: "Cheese", quantity: 120, price 300})
```

`insert-multi!` inserts multiple documents, and returns inserted documents.

```clojure
(insert-multi! :foods
               {:name "Cookie" :quantity 70 :price 120}
               {:name "Banana" :quantity 40 :price 100}
               {:name "Chunky Bacon" :quantity 600 :price 800})
```

### Fetching

`query` makes a query behaved like a lazy sequence that contains all documents in the specified collection.

```clojure
(query :foods)
; => db.foods.find()
```

The following code prints names of all foods.

```clojure
(doseq [food (query :foods)]
  (println (:name food)))
```

#### Restriction

```clojure
(restrict :name "Cheese" :foods)
; => db.foods.find({name: "Cheese"})
```

```clojure
(restrict :quantity {:$gt 100} :price {:$lt 300} :foods)
; => db.foods.find({quantity: {$gt: 100}, price: {$lt: 300}})
```

You can use following functions as operators in conditions.

    > => < <= mod type not

```clojure
(restrict :quantity {> 100} :price {< 300} :foods)
; => db.foods.find({quantity: {$gt: 100}, price: {$lt: 300}})
```

#### Projection

```clojure
(project :name :price :foods)
; => db.foods.find({}, {name: 1, price: 1})
```

```clojure
(project :name :price (restrict :price {> 100} :foods))
; => db.foods.find({price: {$gt: 100}}, {name: 1, price: 1})
```

```clojure
(project {:price false :quantity false} :foods)
; => db.foods.find({}, {price: 0, quantity: 0})
```

#### Sort



```clojure
(order :price 1 :foods)
; => db.foods.find().sort({price: 1})
```

You can use :asc and :desc instead of 1 and -1.

```clojure
(order :price :desc :name :asc (restrict :quantity {< 100} :foods))
; => db.foods.find({quantity: {$lt: 100}}).sort({price: -1, name: 1})
```

`reverse-order` reverses the order of the specified query.

```clojure
(reverse-order (order :price :asc :name :desc :foods))
; => db.foods.find().sort({price: -1, name: 1})
```

#### Limit

```clojure
(limit 3 :foods)
; => db.foods.find().limit(3)
```

```clojure
(limit 2 (order :price :asc (restrict :price {> 50} :foods)))
; => db.foods.find({price: {$gt: 50}}).sort({price: 1}).limit(2)
```

#### Skip

```clojure
(skip 2 :foods)
; => db.foods.find().skip(2)
```

```clojure
(skip 3 (order :price :asc :foods))
; => db.foods.find().sort({price: 1}).skip(3)
```

#### Applying function

`postapply` applies the specified function to documents returned from the specified query, that is, `postapply` behaves like `apply`, but `postapply` returns a new query.

```clojure
(postapply #(partition-all 2 %) :foods)

(postapply #(filter (fn [document] (odd? (:number document))) (restrict :type 1 :users)))
```

A function passed to `postapply` must return a sequence.

You can pass a query returned from `postapply` to other functions that receive a query.

#### Mapping

`map-after` applies the specified function to each document returned from the specified query, that is, `map-after` behaves like `map`, but `map-after` returns a new query instead of a sequence of objects.

```clojure
(map-after #(assoc :discounted-price (* (:price %) 0.8))
           (restrict :price {> 100} :foods))

You can pass a query returned from `map-after` to other functions that receive a query.

```clojure
(restrict :price {> 100}
          (map-after #(assoc :discounted-price (* (:price %) 0.8)) :foods))
```

#### Counting

```clojure
(count (restrict :price {> 100} :oods))
```

MongoDB does not return any documents when `count` is called.

#### Setting query options

`query-options` sets query options to the specified query.

```clojure
(query-option com.mongodb.Bytes/QUERYOPTION_NOTIMEOUT :foods)
```

You can pass keywords instead of numbers.

```clojure
(query-option :notimeout :foods)
```

### Update

`update!` updates just one document that are returned from the specified query.

```clojure
(update! :$set {:quantity 80} (restrict :name "Banana" :foods))
; => db.foods.update({name: "Banana"}, {$set: {quantity: 80}}, false, false)
```

```clojure
(update! :$set {:quantity 80} :$inc {:price 10} (restrict :name "Banana" :foods))
; => db.foods.update({name: "Banana"}, {$set: {quantity: 80}, $inc: {:price 10}}, false, false)
```

`upsert!` does an "upsert" operation.

```clojure
(upsert! :$set {:price 100 :quantity 80} (restrict :name "Cheese" :foods))
; => db.foods.update({name: "Cheese"}, {$set: {price: 100, autntity: 80}}, true, false)
```

`update-multi!` updates all documents that are returned from the specified query.

```clojure
(update-multi! :$inc {:price 10} :foods)
; => db.foods.update({}, {$inc: {price: 10}}, false, true)
```

`upsert-multi!` does an "upsert" operation with true as "multi" parameter.


```clojure
(upsert-multi! :$inc {:price 10} :foods)
; => db.foods.update({}, {$inc: {price: 10}}, true, true)
```

### Deletion

`delete!` removes all documents that are returned from the specified query.

```clojure
(delete! (restrict :price {< 100} :foods))
; => db.foods.remove({price: {$lt: 100}})
```

`delete-one!` removes just one document and returns it.

```clojure
(delete-one! (restrict :price {< 100} :foods))
; => db.foods.findAndModify({query: {price: {$lt: 100}}, remove: true})
```

### Index

`ensure-index!` creates an index.

```clojure
(ensure-index! :foods {:category :asc})
(ensure-index! :foods {:name :asc} :unique true)
(ensure-index! :foods [:price :asc :name :asc])
(ensure-index! :users {:rank :desc} :name :user-rank-desc)
```

### MapReduce

`map-reduce!` invoke mapReduce command with query and following options:
- map: map function as a JavaScript code
- reduce: reduce function as a JavaScript code
- finalize: finalize function as a JavaScript code
- out: name of collection to output to
- out-type: replace/merge/reduce
- scope: variables to use in map/reduce/finalize functions
- verbose

The query can contain restriction, limit and sorting.

```clojure
(map-reduce! :map "function() {
                     emit(this.color, this.price);
                   }"
             :reduce "function(key, vals) {
                        var sum = 0;
                        for (var i = 0; i < vals.length; ++i) sum += vals[i];
                        return sum;
                      }"
             :out :sum-of-prices
             :fruits)
```

```clojure
(map-reduce! :map "function() {
                     emit(this['item-id'], 1);
                   }"
             :reduce "function(key, vals) {
                        var count = 0;
                        for (var i = 0; i < vals.length; ++i) count += vals[i];
                        return count;
                      }"
             :out :sell-count
             :out-type :reduce
             (restrict :date {>= first-of-month} :date {< first-of-next-month} :fruits))
```

### GridFS

`grid-fs` returns a GridFS instance. You can pass it to functions as a query.

```clojure
(grid-fs :images)
```

```clojure
(query (grid-fs :images))
```

```clojure
(restrict :_id image-id (grid-fs :images))
```

```clojure
(count (restrict :metadata.width {> 200} (grid-fs :images)))
```

A map returned from a GridFS query has a `:data` field, and it's value is an InputStream of bytes.

You can use `insert!`, `insert-multi!` and `delete!` for GridFS, but `update!`, `upsert!` and `update-multi!` does not support GridFS.

```clojure
(insert! {:data byte-array-or-iterator
          :filename "image1.png"
          :contentType "image/png"}
         (grid-fs :images))
```

```clojure
(delete! (restrict :_id image-id (grid-fs :images)))
```

### Others

`collection-names` returns names of collections in the current bound database.

```clojure
(collection-names)
```

`collection-exists?` returns if the specified collection exists.

```clojure
(collection-exists? :items)
```

`collection-stats` returns stats of the specified collection.

```clojure
(collection-stats :items)
```

## Install

Add

```clojure
[mongoika "0.8.5"]
```

to your project.clj.

## Mongoika?

Mongoika is named from a cuttlefish called "Mongou Ika" (紋甲イカ) in Japanese.

[LogoImage]: https://raw.github.com/yuushimizu/Mongoika/master/logo.png
