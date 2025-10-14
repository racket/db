#lang racket/base
(require racket/class
         db/private/generic/interfaces
         db/private/generic/common)
(provide kill-safe-connection
         virtual-connection
         connection-pool
         connection-pool?
         connection-pool-lease)

;; channel-call : Channel (-> X) b:Boolean -> (if b (Evt X) X)
(define (channel-call chan proc as-evt?)
  (define result #f) ;; mutated
  (define sema (make-semaphore 0))
  (define (wrapped-proc)
    (set! result
          (with-handlers ([(lambda (e) #t)
                           (lambda (e) (cons 'exn e))])
            (cons 'values (call-with-values proc list))))
    (semaphore-post sema))
  (define (handler _evt)
    (semaphore-wait sema)
    (case (car result)
      ((values) (apply values (cdr result)))
      ((exn) (raise (cdr result)))))
  (if as-evt?
      (wrap-evt (channel-put-evt chan wrapped-proc) handler)
      (begin (channel-put chan wrapped-proc) (handler #f))))

;; manager% implements kill-safe manager thread w/ request channel
(define manager%
  (class object%
    ;; other-evt : (-> evt)
    ;; generates other evt to sync on besides req-channel, eg timeouts
    (init-field (other-evt (lambda () never-evt)))
    (super-new)

    (define req-channel (make-channel))

    (define mthread
      (thread/suspend-to-kill
       (lambda ()
         (let loop ()
           (sync (wrap-evt req-channel (lambda (p) (p)))
                 (other-evt))
           (loop)))))

    (define/public (resume)
      (thread-resume mthread (current-thread)))
    (define/public (call proc)
      (resume)
      (channel-call req-channel proc #f))
    (define/public (call-evt proc)
      (resume)
      (channel-call req-channel proc #t))
    ))

;; ----

;; Kill-safe wrapper

;; Note: wrapper protects against kill-thread, but not from
;; custodian-shutdown of ports, etc.

(define kill-safe-connection%
  (class* object% (connection<%>)
    (init-private connection)

    (define mgr (new manager%))
    (define last-connected? #t)

    (define-syntax-rule (define-forward (method arg ...) ...)
      (begin
        (define/public (method arg ...)
          (send mgr call (lambda ()
                           (dynamic-wind
                             void
                             (lambda () (send connection method arg ...))
                             (lambda () (set! last-connected? (send connection connected?)))))))
        ...))

    (define/public (connected?)
      ;; If mgr is busy, then just return last-connected?, otherwise, do check.
      (sync/timeout
       (lambda () last-connected?)
       (send mgr call-evt
             (lambda ()
               (set! last-connected? (send connection connected?))
               last-connected?))))

    (define-forward
      (disconnect)
      (get-dbsystem)
      (query fsym stmt cursor?)
      (prepare fsym stmt close-on-exec?)
      (fetch/cursor fsym cursor fetch-size)
      (get-base)
      (free-statement stmt need-lock?)
      (transaction-status fsym)
      (start-transaction fsym isolation option cwt?)
      (end-transaction fsym mode cwt?)
      (list-tables fsym schema))

    (super-new)))

;; ----

(define (kill-safe-connection connection)
  (new kill-safe-connection%
       (connection connection)))

;; ========================================

;; Virtual connection

(define virtual-connection%
  (class* object% (connection<%>)
    (init-private connector     ;; called from client thread
                  get-key)      ;; called from client thread
    (super-new)

    (define custodian (current-custodian))

    ;; == methods called in manager thread ==

    ;; key=>conn : hasheq[key => connection]
    (define key=>conn (make-hasheq))

    (define/private (get key)      ;; also called by client thread for connected?
      (hash-ref key=>conn key #f))

    (define/private (put! key c)
      (hash-set! key=>conn key c))

    (define/private (remove! key)
      (let ([c (get key)])
        (when c
          (hash-remove! key=>conn key)
          (send c disconnect))))

    (define mgr
      (new manager%
           (other-evt
            (lambda ()
              (choice-evt
               (let ([keys (hash-map key=>conn (lambda (k v) k))])
                 (handle-evt (apply choice-evt keys)
                             ;; Assignment to key has expired
                             (lambda (key)
                               (log-db-debug "virtual-connection: key expiration: ~e" key)
                               (remove! key)))))))))

    ;; == methods called in client thread ==

    (define/private (get-connection create?)
      (let* ([key (get-key)]
             [c (send mgr call (lambda () (get key)))])
        (cond [(and c (send c connected?)) c]
              [create?
               (log-db-debug
                (if c
                    "virtual-connection: refreshing connection (old is disconnected)"
                    "virtual-connection: creating new connection"))
               (let ([c* (parameterize ((current-custodian custodian))
                           (connector))])
                 (send mgr call
                       (lambda ()
                         (when c (remove! key))
                         (put! key c*)))
                 c*)]
              [else
               (when c ;; got a disconnected connection
                 (send mgr call (lambda () (remove! key))))
               #f])))

    ;; ----

    (define-syntax-rule (define-forward (req-con? no-con (method arg ...)) ...)
      (begin (define/public (method arg ...)
               (let ([c (get-connection req-con?)])
                 (if c
                     (send c method arg ...)
                     no-con)))
             ...))

    (define-forward
      (#t '_     (get-dbsystem))
      (#t '_     (query fsym stmt cursor?))
      (#t '_     (fetch/cursor fsym stmt fetch-size))
      (#t '_     (start-transaction fsym isolation option cwt?))
      (#f (void) (end-transaction fsym mode cwt?))
      (#f #f     (transaction-status fsym))
      (#t '_     (list-tables fsym schema)))

    (define/public (get-base)
      (get-connection #t))

    (define/public (connected?)
      (let ([c (get (get-key))])
        (and c (send c connected?))))

    (define/public (disconnect)
      (let ([c (get-connection #f)]
            [key (get-key)])
        (when c
          (send c disconnect)
          (send mgr call (lambda () (remove! key)))))
      (void))

    (define/public (prepare fsym stmt close-on-exec?)
      ;; FIXME: hacky way of supporting virtual-statement
      (unless (or close-on-exec? (eq? fsym 'virtual-statement))
        (error fsym "cannot prepare statement with virtual connection"))
      (send (get-connection #t) prepare fsym stmt close-on-exec?))

    (define/public (free-statement stmt need-lock?)
      (error/internal 'free-statement "virtual connection does not own statements"))))

;; ----

(define (virtual-connection connector)
  (let ([connector
         (cond [(connection-pool? connector)
                (lambda () (connection-pool-lease connector))]
               [else connector])]
        [get-key (lambda () (thread-dead-evt (current-thread)))])
    (new virtual-connection%
         (connector connector)
         (get-key get-key))))

;; ========================================
;; Connection pool

;; Delay in milliseconds before discarding connections over max-idle limit.
(define DISCARD-DELAY-MS 50.0)

(define connection-pool%
  (class* object% ()
    (init-private connector              ;; called from manager thread
                  max-connections
                  max-idle
                  max-idle-ms)
    (super-new)

    ;; ========================================
    ;; methods called in manager thread

    (define proxy-counter 1) ;; for debugging
    (define actual-counter 1) ;; for debugging
    (define actual=>number (make-weak-hasheq))

    (define/private (next-proxy-number)
      (let ([n proxy-counter]) (set! proxy-counter (add1 n)) n))
    (define/private (next-actual-number)
      (let ([n actual-counter]) (set! actual-counter (add1 n)) n))

    ;; proxy=>evt : hasheq[proxy-connection => evt]
    (define proxy=>evt (hasheq))

    ;; lease* : Evt -> Connection/#f
    (define/private (lease* key)
      (cond [(try-take-idle)
             => (lambda (raw-c) (lease** key raw-c #t))]
            [(not (< (hash-count proxy=>evt) max-connections))
             #f]
            [else
             (define raw-c (connector))
             (hash-set! actual=>number raw-c (next-actual-number))
             (lease** key raw-c #f)]))

    (define/private (lease** key raw-c reused?)
      (define proxy-number (next-proxy-number))
      (log-db-debug "connection-pool: leasing connection #~a (~a @~a)"
                    proxy-number (if reused? "idle" "new")
                    (hash-ref actual=>number raw-c "???"))
      (define c (new proxy-connection% (pool this) (connection raw-c) (number proxy-number)))
      (set! proxy=>evt (hash-set proxy=>evt c (wrap-evt key (lambda (_e) c))))
      c)

    (define/private (release* proxy raw-c why)
      (log-db-debug "connection-pool: releasing connection #~a (~a, ~a)"
                    (send proxy get-number)
                    (cond [(not raw-c) "no-op"]
                          [(< (hash-count idle-list) max-idle) "idle"]
                          [else "disconnect"])
                    why)
      (set! proxy=>evt (hash-remove proxy=>evt proxy))
      (when raw-c
        ;; If in tx, just disconnect (for simplicity; else must loop for nested txs)
        (define (handle-tx-exn e)
          (log-db-error "connection pool: error from transaction-status: ~e" (exn-message e)))
        (cond [(with-handlers ([exn:fail? handle-tx-exn])
                 (send raw-c transaction-status 'connection-pool))
               (begin (discard-connection raw-c) #t)]
              [else (add-idle! raw-c)])))

    (define/private (discard-connection raw-c)
      (log-db-debug "connection pool: discarding connection @~a"
                    (hash-ref actual=>number raw-c "???"))
      (define (handle e)
        (log-db-error "connection pool: error from disconnect: ~s" (exn-message e)))
      (with-handlers ([exn:fail? handle])
        (send raw-c disconnect)))

    ;; ----------------------------------------
    ;; idle list

    ;; idle-list : hasheq[raw-connection => monotonic-time-ms]
    (define idle-list (hasheq))

    ;; soonest-timeout-ms : monotonic-time-ms
    (define soonest-timeout-ms +inf.0)

    ;; discard-excess-ms : monotonic-time-ms
    (define discard-excess-ms +inf.0)

    (define/private (add-idle! raw-c)
      (define now (current-inexact-monotonic-milliseconds))
      (define timeout-ms (+ now max-idle-ms))
      (set! idle-list (hash-set idle-list raw-c timeout-ms))
      (set! soonest-timeout-ms (min soonest-timeout-ms timeout-ms))
      (when (> (hash-count idle-list) max-idle)
        (set! discard-excess-ms (min discard-excess-ms (+ now DISCARD-DELAY-MS)))))

    (define/private (try-take-idle)
      (define c (for/first ([c (in-hash-keys idle-list)]) c))
      (when c (set! idle-list (hash-remove idle-list c)))
      (and c (if (send c connected?) c (try-take-idle))))

    (define/private (trim-idle-excess)
      (unless (< (hash-count idle-list) max-idle)
        (define to-discard (- (hash-count idle-list) max-idle))
        (set! idle-list (for/fold ([idle idle-list])
                                  ([i (in-range to-discard)]
                                   [c (in-hash-keys idle-list)])
                          (discard-connection c)
                          (hash-remove idle c)))
        (set! discard-excess-ms +inf.0)))

    (define/private (trim-idle-timeouts)
      (define now (current-inexact-monotonic-milliseconds))
      (define-values (new-idle-list min-timeout-ms)
        (for/fold ([idle idle-list] [min-timeout-ms +inf.0])
                  ([(c timeout-ms) (in-hash idle-list)])
          (cond [(<= timeout-ms now)
                 (discard-connection c)
                 (values (hash-remove idle c) min-timeout-ms)]
                [else (values idle (min min-timeout-ms timeout-ms))])))
      (set! idle-list new-idle-list)
      (set! soonest-timeout-ms min-timeout-ms))

    (define/private (clear-idle*)
      (set! soonest-timeout-ms +inf.0)
      (set! discard-excess-ms +inf.0)
      (for ([c (in-hash-keys idle-list)])
        (discard-connection c))
      (set! idle-list (hasheq)))

    ;; Blocking lease requests use this channel, and the manager only accepts
    ;; the requests when it is able to fill them.
    (define lease-channel (make-channel))

    (define mgr
      (new manager%
           (other-evt
            (lambda ()
              (choice-evt
               (cond [(< (hash-count proxy=>evt) max-connections)
                      (wrap-evt lease-channel (lambda (p) (p)))]
                     [else never-evt])
               (handle-evt (apply choice-evt (hash-values proxy=>evt))
                           (lambda (proxy)
                             (release* proxy
                                       (send proxy release-connection)
                                       "release-evt")))
               (handle-evt (alarm-evt discard-excess-ms #t)
                           (lambda (_evt) (trim-idle-excess)))
               (handle-evt (alarm-evt soonest-timeout-ms #t)
                           (lambda (_evt) (trim-idle-timeouts))))))))

    ;; ========================================
    ;; methods called in client thread

    (define/public (lease-evt key block?)
      (cond [block?
             (send mgr resume)
             (channel-call lease-channel (lambda () (lease* key)) #t)]
            [else
             (send mgr call-evt (lambda () (lease* key)))]))

    (define/public (release proxy)
      (let ([raw-c (send proxy release-connection)])
        (send mgr call (lambda () (release* proxy raw-c "proxy disconnect"))))
      (void))

    (define/public (clear-idle)
      (send mgr call (lambda () (clear-idle*))))

    ))

;; --

(define proxy-connection%
  (class* locking% (connection<%>)
    (init-private connection
                  pool
                  number)
    (inherit call-with-lock)
    (super-new)

    (define-syntax-rule (define-forward defmethod (method arg ...) ...)
      (begin
        (defmethod (method arg ...)
          (call-with-lock 'method
            (lambda ()
              (let ([c connection])
                (unless c (error/not-connected 'method))
                (send c method arg ...)))))
        ...))

    (define-forward define/public
      (get-dbsystem)
      (query fsym stmt cursor?)
      (prepare fsym stmt close-on-exec?)
      (fetch/cursor fsym stmt fetch-size)
      (get-base)
      (free-statement stmt need-lock?)
      (transaction-status fsym)
      (start-transaction fsym isolation option cwt?)
      (end-transaction fsym mode cwt?)
      (list-tables fsym schema))

    (define/override (connected?) (and connection (send connection connected?)))

    (define/public (disconnect)
      (send pool release this))

    (define/public (get-number) number)

    (define/public (release-connection)
      (begin0 connection
        (set! connection #f)))))

;; ----

(define (connection-pool connector
                         #:max-connections [max-connections +inf.0]
                         #:max-idle-connections [max-idle 10]
                         #:max-idle-seconds [max-idle-s 300])
  (new connection-pool%
       (connector connector)
       (max-connections max-connections)
       (max-idle max-idle)
       (max-idle-ms (* 1000.0 max-idle-s))))

(define (connection-pool? x)
  (is-a? x connection-pool%))

(define (connection-pool-lease pool [key0 (current-thread)]
                               #:timeout [timeout-s #f]
                               #:fail [fail
                                       (lambda ()
                                         (error 'connection-pool-lease
                                                "connection pool limit reached"))])
  (define key
    (cond [(thread? key0) (thread-dead-evt key0)]
          [(custodian? key0) (make-custodian-box key0 #t)]
          [else key0]))
  (or (sync/timeout timeout-s (send pool lease-evt key (and timeout-s #t)))
      (if (procedure? fail) (fail) fail)))
