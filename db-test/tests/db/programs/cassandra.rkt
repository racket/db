#lang racket/base
(require db rackunit racket/format racket/list racket/math racket/date racket/set)

;; FIXME: use dsn instead?
(define c
  (with-handlers ([exn:fail? (lambda (e)
                               (printf "Cannot connect to Cassandra. Skipping tests.\n")
                               (exit 0))])
    (cassandra-connect #:debug? #f)))

(query-exec c "drop keyspace if exists testkeysp")

(query-exec c (~a "create keyspace testkeysp "
                  "with replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};"))
(query-exec c "use testkeysp")
(query-exec c "drop table if exists the_numbers")
(query-exec c "create table if not exists the_numbers (n int primary key, t text)")

(check-exn #rx"Undefined column name"
           (lambda () (query-value c "select nope from the_numbers where n = 0")))
(check-exn #rx"Undefined column name"
           (lambda () (prepare c "select nope from the_numbers where n = ?")))

(define test-data
  '((0 "nothing")
    (1 "unity")
    (2 "the loneliest number since the number one")
    (4 "four")
    (5 "five")
    (6 "half a dozen")))

(for ([p test-data])
  (query-exec c "insert into the_numbers (n, t) values (?, ?)" (car p) (cadr p)))

(check-equal? (query-value c "select t from the_numbers where n = ?" 0)
              (cadr (assv 0 test-data)))

(define get-t-query (prepare c "select t from the_numbers where n = ?"))

(check-equal? (query-value c get-t-query 1)
              (cadr (assv 1 test-data)))
(check-equal? (query-maybe-value c get-t-query 12)
              #f)
(check-equal? (query-list c get-t-query 4)
              (list (cadr (assv 4 test-data))))
(check-equal? (query-row c get-t-query 5)
              (vector (cadr (assv 5 test-data))))

(define (test-type type . vals)
  (test-case type
    (query-exec c (format "create table testrt (n int primary key, v ~a)" type))
    (for ([i (in-naturals)] [v vals])
      (query-exec c "insert into testrt (n, v) values (?, ?)" i v)
      (check-equal? (query-value c "select v from testrt where n = ?" i) v)))
  (void (query-exec c "drop table if exists testrt")))

(test-type "ascii" "" "abc" (make-string 20 #\A))
(test-type "ascii" (make-string #e1e5 #\a))
(test-type "bigint" 123456789012345678 -123456789012345678)
(test-type "blob" #"" (apply bytes (range 256)))
(test-type "boolean" #t #f)
(test-type "double" 0.0 1.0 0.5 pi (sin 1) (cos 1) +nan.0 +inf.0 -inf.0)
(test-type "float" 0.0 1.0 0.5) ;; FIXME
(test-type "text" "abc" "λambdα" "děkuju")
(test-type "timestamp" (sql-timestamp 2020 06 25 12 34 56 0 #f))
(test-type "uuid" "86449e66-0b78-4c6f-bf52-5c9ba1699f83")
(define (fact n) (for/product ([i n]) (add1 i)))
(test-type "varint" 0 -1 (fact 10) (fact 20) (fact 50) (fact 80) (- (fact 99)))
;;(test-type "LIST<int>" '()) ;; '() comes back as sql-null!
(test-type "LIST<int>" '(0) '(1 2 3) '(1 -2 3 -4) (range 1000))
(test-type "SET<int>" (set 0) (set 1 2 3) (set 1 -2 3 -4) (list->set (range 1000)))
;;(test-type "MAP<int,ascii>" '()) ;; '() comes back as sql-null!
(test-type "MAP<int,ascii>" '((1 . "a") (2 . "bc") (3 . "def")))
(test-type "TUPLE<int,ascii>" (vector 1 "a") (vector 2 "bc"))

(query-exec c "drop keyspace if exists testkeysp")
(disconnect c)
