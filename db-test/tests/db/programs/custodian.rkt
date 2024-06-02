#lang racket/base
(require db
         racket/cmdline
         racket/match)

;; This tests custodian shutdowns connections.
;; Success indicates that a custodian shutdown
;; - doesn't crash
;; - doesn't deadlock (due to call-with-lock in atomic mode)
;;   - if DrDr times out, may indicate deadlock
;; - can interrupt a db operation (query)
;;   - checked by exn message

;; Usage: racket custodian.rkt [dsn]
;; Test sqlite3 if no dsn, test dsn if present.

(define dsn #f)
(define verbose? #f)

(command-line
 #:once-each
 [("-v" "--verbose") "Verbose" (set! verbose? #t)]
 #:args ([dsn-string #f])
 (when dsn-string
   (set! dsn (string->symbol dsn-string))))

(when verbose?
  (printf "Running tests for ~a.\n" (or dsn 'sqlite3)))

(define shutdown-exn? #f)

(define (run-test counter)
  (define cust (make-custodian))
  (define c
    (parameterize ((current-custodian cust))
      (if dsn
          (dsn-connect dsn)
          (sqlite3-connect #:database 'memory))))
  (define result #f)
  (define th
    (thread
     (lambda ()
       (with-handlers ([values (lambda (e) (set! result e))])
         (let loop ()
           ;; We want a query that holds the lock for a long time (so we have a
           ;; chance to interrupt it) but doesn't spend all that time in the
           ;; foreign call (so the interrupting thread actually gets to run).
           (query c
             (string-append "with recursive ns(n) as ( select 1 union select n+1 from ns ) "
                            "select n from ns limit ?") counter)
           (loop))))))
  (sleep (random))
  (custodian-shutdown-all cust)
  (sleep 0.01)
  (define sr (sync/timeout 0.01 th))
  (cond [(eq? sr #f)
         (error 'test "thread still running")]
        [else
         (unless (exn? result)
           (error 'test "didn't get an exception"))
         (when (connected? c)
           (error 'test "still connected after shutdown"))
         (when (regexp-match #rx"disconnected during operation" (exn-message result))
           (set! shutdown-exn? #t))
         (when verbose?
           (printf "... got exn: ~.s\n" (exn-message result)))]))

(for ([i 100]
      #:break shutdown-exn?)
  (when verbose? (printf "Test run ~s" i))
  (run-test i))

(unless shutdown-exn?
  (error 'test "did not see expected exception, 'disconnected during operation'"))
(when verbose? (printf "Success\n"))
