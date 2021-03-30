#lang racket/base
(require racket/runtime-path
         racket/format
         rackunit
         ffi/unsafe/os-thread
         db
         db/unsafe/sqlite3)
(provide (all-defined-out))

(define-runtime-path sqlite3-ext "sqlite3ext.so")

;; == Test load-extension ==
;; This test only runs if sqlite3ext.so exists. See sqlite3ext.c for
;; compilation instructions.

(define (run-tests c)

  (cond [(file-exists? sqlite3-ext)
         (sqlite3-load-extension c sqlite3-ext)
         (check-equal? (query-value c "select extplus(?,?)" 1.0 2.0)
                       3.0)
         (check-equal? (query-value c "select extplus(?,?)" 3 4.0)
                       7.0)
         (check-equal? (query-value c "select extplus(?,?)" 5.0 sql-null)
                       sql-null)
         (check-exn #rx"wanted a number"
                    (lambda () (query-value c "select extplus(?,?)" 6.0 "seven")))]
        [else
         (printf "skipping extension test (sqlite3ext.so does not exist)\n")])

  ;; == Test create-function ==

  (sqlite3-create-function c "rktplus" #f +)

  (check-equal? (query-value c "select rktplus(?,?)" 1 2.0)
                3.0)

  ;; Racket and RacketCS have different exception messages here:
  (check-exn #rx"contract violation|is not a number"
             (lambda () (query-value c "select rktplus('abc',2)")))

  ;; == Test create-aggregate ==

  (sqlite3-create-aggregate c "rktstrapp" #f "" string-append values)
  (sqlite3-create-aggregate c "rktconcat" #f "" ~a values)

  (check-equal? (query-value c "select rktstrapp('abc','de','f')")
                "abcdef")

  (check-exn #rx"contract violation"
             (lambda () (query-value c "select rktstrapp('abc','d',5,'f')")))

  (check-equal? (query-value c "select rktconcat(1,2,'c','d')")
                "12cd")

  (query-exec c "create temporary table xs (x any)")
  (for ([x '(1 2 3 4 5)])
    (query-exec c "insert into xs (x) values (?)" x))
  (check-equal? (query-value c "select rktconcat(x) from xs")
                "12345")

  (void))

(printf "Testing with use-place=#f\n")
(run-tests (sqlite3-connect #:database 'memory #:use-place #f))

(when (os-thread-enabled?)
  (printf "Testing with use-place=os-thread\n")
  (run-tests (sqlite3-connect #:database 'memory #:use-place 'os-thread)))
