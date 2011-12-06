![][LogoImage]

# Mongoika

A MongoDB Library for Clojure.

## Example

```clojure
;; Use mongoika namespace.
(use 'mongoika)
    
;; Connect to a MongoDB server.
(with-mongo [mongo {:host your-mongodb-host :port your-mongodb-port}]
  ;; Dynamic bind a database object.
  (with-db-binding (database mongo :your-db)
    ;; Insertion:
    (insert! :fruits {:name "Banana" :color :yellow :price 100})
    (insert! :fruits {:name "Apple" :color :red :price 80})

    ;; Multiple insertion:
    (insert-multi! :fruits
                   {:name "Lemon" :color :yellow :price 50}
                   {:name "Strawberry" :color :red :price 200})

    ;; Fetch all:
    (query :fruits)
    ; => [banana apple lemon strawberry]

    ;; Find:
    (restrict :color :red :fruits)
    ; => [apple strawberry]
    
    (restrict :price {> 100} :fruits)
    ; => [banana strawberry]

    ;; Sort:
    (order :price :asc :fruits)
    ; => [lemon apple banana strawberry]

    ;; Find and sort:
    (order :price :desc (restrict :color :yellow :fruits))
    ; => [banana lemon]

    ;; Fetch first:
    (fetch-one (order :price :desc (restrict :color :yellow :fruits)))))
    ; => banana
```
## Usage
### Connect to a MongoDB server.

```clojure
(mongo {:host "127.0.0.1" :port 27017})
```

```clojure
; Connect with options.
(mongo {:host "127.0.0.1" :port 27017} {:safe true :socketTimeout 5})
```

## Install

Add

```clojure
[mongoika "0.6"]
```
to your project.clj.

## Mongoika?

Mongoika is named from a cuttlefish called "Mongou Ika" (紋甲イカ) in Japanese.

[LogoImage]: https://raw.github.com/yuushimizu/Mongoika/master/logo.png
