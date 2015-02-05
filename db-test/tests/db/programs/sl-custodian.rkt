#lang racket/base
(require db)

;; This tests custodian shutdowns for sqlite3 connections.
;; Success indicates that a custodian shutdown
;; - doesn't crash
;; - doesn't deadlock (due to call-with-lock in atomic mode)
;;   - if DrDr times out, may indicate deadlock
;; - can interrupt a db operation (query)
;;   - checked by exn message

(define counter 0)
(define shutdown-exn? #f)

(define (run-test)
  (define cust (make-custodian))
  (define c
    (parameterize ((current-custodian cust))
      (sqlite3-connect #:database 'memory)))
  (define result #f)
  (define th
    (thread
     (lambda ()
       (with-handlers ([values (lambda (e) (set! result e))])
         (let loop ()
           (query c "select ?" counter)
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
         (printf "... got exn: ~.s\n" (exn-message result))]))

(for ([i 100]
      #:break shutdown-exn?)
  (printf "Test run ~s" i)
  (run-test))

(unless shutdown-exn?
  (error 'test "did not see expected exception, 'disconnected during operation'"))
(printf "Success\n")
