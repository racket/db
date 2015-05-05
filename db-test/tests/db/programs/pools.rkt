#lang racket/base
(require rackunit
         db)

;; make-connections : Connection-Pool (Listof Box) -> Void
(define (make-connections pool bs)
  (for ([b bs])
    (thread
     (lambda ()
       (with-handlers ([exn:fail?
                        (lambda (e)
                          (set-box! b 'error)
                          (when #f (raise e)))])
         (set-box! b
           (connection-pool-lease pool (current-custodian))))))))

;; do-connect : -> Connection
(define (do-connect)
  (sleep 0.1) ;; slow down
  (sqlite3-connect #:database 'memory))

(let ()
  (define pool-no-limit
    (connection-pool do-connect))
  (define bs (for/list ([i 25]) (box #f)))
  (make-connections pool-no-limit bs)
  (sleep 4)
  (check-equal? (for/sum ([b bs] #:when (connection? (unbox b))) 1) 25))

(let ()
  (define pool-with-limit
    (connection-pool do-connect #:max-connections 10))
  (define bs (for/list ([i 25]) (box #f)))
  (make-connections pool-with-limit bs)
  (sleep 4)
  (check-equal? (for/sum ([b bs] #:when (connection? (unbox b))) 1) 10)
  (check-equal? (for/sum ([b bs] #:when (eq? (unbox b) 'error)) 1) 15))
