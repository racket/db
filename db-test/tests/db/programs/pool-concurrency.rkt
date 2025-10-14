#lang racket/base
(require racket/class
         racket/match
         racket/cmdline
         racket/runtime-path
         db)

(module test racket/base)

;; Benchmark for connection pools

(define-runtime-path testing-dsn "../test-dsn.rktd")
(current-dsn-file testing-dsn)

(define QUERY "SELECT 42")

(define DSN 'pg) ;; mutated
(define CONCURRENCY 1) ;; mutated
(define POOL-SIZE 100)
(define ITERATIONS 1000)

(command-line
 #:once-each
 [("-d" "--dsn") dsn "Use given DSN" (set! DSN (string->symbol dsn))]
 #:args (concurrency)
 (set! CONCURRENCY (string->number concurrency))
 (unless (exact-positive-integer? CONCURRENCY)
   (error 'pool-concurrency "expected positive integer, got ~e" CONCURRENCY)))

(define pool
  (connection-pool
   #:max-connections POOL-SIZE
   (lambda () (dsn-connect DSN))))

(define sema (make-semaphore CONCURRENCY))
(define (test q)
  (for-each
   thread-wait
   (for/list ([_ (in-range ITERATIONS)])
     (thread
      (lambda ()
        (call-with-semaphore sema
          (lambda ()
            (define conn (connection-pool-lease pool))
            (query-row conn q)
            (disconnect conn))))))))

(let ()
  (collect-garbage)
  (collect-garbage)
  (time (test QUERY)))
