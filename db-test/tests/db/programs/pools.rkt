#lang racket/base
(require rackunit
         db)

;; make-connections : Connection-Pool (Listof Box) -> Void
(define (make-connections pool bs)
  (for-each
   thread-wait
   (for/list ([b bs])
     (thread
      (lambda ()
        (with-handlers ([exn:fail?
                         (lambda (e)
                           (set-box! b 'error)
                           (when #f (raise e)))])
          (set-box! b (connection-pool-lease pool (current-custodian) #:timeout 1))))))))

;; do-connect : -> Connection
(define (do-connect)
  (sleep 0.01) ;; slow down
  (sqlite3-connect #:database 'memory))

(define (call/custodian-shutdown proc)
  (define cust (make-custodian))
  (parameterize ((current-custodian cust))
    (dynamic-wind void proc (lambda () (custodian-shutdown-all cust)))))

(call/custodian-shutdown
 (lambda ()
   (define pool-no-limit
     (connection-pool do-connect))
   (define bs (for/list ([i 25]) (box #f)))
   (make-connections pool-no-limit bs)
   (check-equal? (for/sum ([b bs] #:when (connection? (unbox b))) 1) 25)))

(call/custodian-shutdown
 (lambda ()
   (define pool-with-limit
     (connection-pool do-connect #:max-connections 10))
   (define bs (for/list ([i 25]) (box #f)))
   (make-connections pool-with-limit bs)
   (check-equal? (for/sum ([b bs] #:when (connection? (unbox b))) 1) 10)
   (check-equal? (for/sum ([b bs] #:when (eq? (unbox b) 'error)) 1) 15)))

(call/custodian-shutdown
 (lambda ()
   (define pool (connection-pool do-connect))
   (define sema (make-semaphore 0))
   (define cs (for/list ([i 20])
                (begin0 (connection-pool-lease pool sema)
                  (when (even? i) (semaphore-post sema)))))
   (sleep 1)
   (check-equal? (for/sum ([c cs]) (if (connected? c) 1 0)) 10)))
