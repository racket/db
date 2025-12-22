#lang racket/base
(require db
         ffi/unsafe/os-thread
         (only-in db/private/generic/interfaces log-db-debug))

;; This program is a non-automated test of custodian shutdown of
;; sqlite3 connections with an active OS thread.

;; Run with PLTSTDERR="debug@db"
;; Expect the following pattern:
;;   db: disconnect/stage2 in worker thread
;;   db: disconnect/stage3 skipped
;;   db: interrupted: "sqlite3: disconnected during operation...."

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
  (with-handlers ([exn:fail?
                   (lambda (e)
                     (log-db-debug "interrupted: ~e" (exn-message e)))])
    (define result (query-value c pst))
    (log-db-debug "not interrupted, result = ~e" result))
  (void))
