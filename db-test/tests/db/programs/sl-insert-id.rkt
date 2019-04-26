#lang racket/base
(require db racket/dict rackunit)

(define c (sqlite3-connect #:database 'memory))

(query-exec c "create table cs (c integer primary key)")
(query-exec c "create table bs (b integer primary key)")

(define qr1 (query c "insert into cs (c) values (?)" 12))
(define qr2 (query c "insert into bs (b) values (?)" 12))
(define qr3 (query c "delete from cs"))

(check-pred simple-result? qr1)
(check-pred simple-result? qr2)
(check-pred simple-result? qr3)
(check-equal? (dict-ref (simple-result-info qr1) 'insert-id) 12)
(check-equal? (dict-ref (simple-result-info qr2) 'insert-id) 12)
(check-equal? (dict-ref (simple-result-info qr3) 'insert-id) #f)
