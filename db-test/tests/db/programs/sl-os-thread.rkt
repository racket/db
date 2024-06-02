#lang racket/base
(require db
         ffi/unsafe/os-thread
         (only-in db/private/generic/interfaces log-db-debug))

;; This program is a non-automated test of custodian shutdown of
;; sqlite3 connections with an active OS thread.

;; Run with PLTSTDERR="debug@db"
;; Expect the following pattern:
;;   db: disconnect delayed by OS thread
;;   (pause)
;;   db: result = ...
;;   db: continuing delayed disconnect

(unless (os-thread-enabled?)
  (printf "ffi/unsafe/os-thread: not supported\n")
  (exit 0))

(define the-sql
  "select max(a.n * b.n *c.n * d.n) from nums a, nums b, nums c, nums d")

(for ([i 3])
  (define cust (make-custodian))
  (define c (parameterize ((current-custodian cust))
              (sqlite3-connect #:database 'memory #:use-place 'os-thread)))
  (query-exec c "create temporary table nums (n integer)")
  (for ([i (in-range 80)])
    (query-exec c "insert into nums (n) values (?)" i))
  (define pst (prepare c the-sql))

  (define sema (make-semaphore 0))
  (define thd
    (thread
     (lambda ()
       (semaphore-post sema)
       (sync (system-idle-evt))
       (custodian-shutdown-all cust))))

  (sync sema)
  (let ([result (query-value c pst)]) ;; eval even if no logging
    (log-db-debug "result = ~s" result))
  (void))
