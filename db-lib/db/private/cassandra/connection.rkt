#lang racket/base
(require racket/class
         racket/match
         db/private/generic/interfaces
         db/private/generic/common
         db/private/generic/sql-data
         db/private/generic/prepared
         "message.rkt"
         "dbsystem.rkt")
(provide connection%)

;; ========================================

(define connection%
  (class* statement-cache% (connection<%>)
    (init-field inport
                outport)
    (field [consistency 'ONE])

    (inherit call-with-lock
             call-with-lock*
             add-delayed-call!
             get-tx-status
             set-tx-status!
             check-valid-tx-status
             check-statement/tx
             tx-state->string
             dprintf
             prepare1)

    (super-new)

    ;; FIXME: flush cache on schema change
    (inherit-field cache-mode)
    (set! cache-mode 'always)

    ;; ========================================
    ;; == Communication

    ;; message-buffer : reversed list of messages waiting to be sent
    (define message-buffer null)

    ;; fresh-exchange : -> void
    (define/private (fresh-exchange)
      (set! message-buffer null))

    ;; buffer-message : message -> void
    (define/private (buffer-message msg)
      (dprintf "  >> ~s\n" msg)
      (set! message-buffer (cons msg message-buffer)))

    ;; flush-message-buffer : -> void
    (define/private (flush-message-buffer)
      (for ([msg (in-list (reverse message-buffer))]
            [streamid (in-naturals 1)])
        (write-message outport msg streamid))
      (set! message-buffer null)
      (flush-output outport))

    ;; send-message : message -> void
    (define/private (send-message msg)
      (buffer-message msg)
      (flush-message-buffer))

    (define/private (call-with-sync fsym proc)
      (with-handlers ([(lambda (e) #t)
                       (lambda (e)
                         ;; Anything but exn:fail:sql (raised by recv-message) indicates
                         ;; a communication error.
                         ;; Alternative: could have check-ready-for-query set a done-reading flag.
                         (unless (exn:fail:sql? e)
                           (disconnect* #f))
                         (raise e))])
        (flush-message-buffer)
        (proc)))

    ;; recv-message : symbol -> message
    (define/private (recv-message who)
      (let ([r (raw-recv)])
        (cond [(Error? r)
               (raise-backend-error who r)]
              [(Event? r)
               (handle-async-message who r)
               (recv-message who)]
              [else r])))

    ;; raw-recv : -> message
    (define/private (raw-recv)
      (match (read-response inport)
        [(cons r streamid)
         (dprintf "  << ~s  @~s\n" r streamid)
         r]))

    ;; == Asynchronous messages

    ;; handle-async-message : message -> void
    (define/private (handle-async-message fsym msg)
      (match msg
        [_ (add-delayed-call! (lambda () (void)))]))

    ;; == Connection management

    ;; disconnect* : boolean -> void
    (define/override (disconnect* politely?)
      (super disconnect* politely?)
      (let ([outport* outport]
            [inport* inport])
        (when outport*
          (close-output-port outport*)
          (set! outport #f))
        (when inport*
          (close-input-port inport*)
          (set! inport #f))))

    ;; connected? : -> boolean
    (define/override (connected?)
      (let ([outport outport])
        (and outport (not (port-closed? outport)))))

    ;; == System

    (define/public (get-dbsystem) dbsystem)

    (define/public (get-base) this)

    ;; ========================================
    ;; == Connect

    ;; start-connection-protocol : -> void
    (define/public (start-connection-protocol)
      (call-with-lock 'cassandra-connect
        (lambda ()
          (send-message (Startup '(("CQL_VERSION" . "3.0.0"))))
          (connect:expect-ready))))

    ;; connect:expect-ready : -> Void
    (define/private (connect:expect-ready)
      (let ([r (recv-message 'cassandra-connect)])
        (match r
          [(Ready)
           (connect:after-auth)]
          [(Authenticate _)
           (error 'cassandra-connect "got Authenticate message: ~e" r)]
          [(AuthChallenge _)
           (error 'cassandra-connect "got AuthChallenge message: ~e" r)]
          [(AuthSuccess _)
           (error 'cassandra-connect "got AuthSuccess message: ~e" r)])))

    ;; connect:after-auth : -> void
    (define/private (connect:after-auth)
      (void))

    ;; ========================================
    ;; == Cache

    (define/override (safe-statement-type? stmt-type) #t)

    ;; ========================================
    ;; == Transactions

    (define/override (start-transaction* who isolation option)
      (error who "transactions not supported"))

    ;; ========================================
    ;; == Prepare

    (define/override (classify-stmt cql) #f)

    (define/override (prepare1* who stmt close-on-exec? stmt-type)
      (fresh-exchange)
      (buffer-message (Prepare stmt))
      (call-with-sync who
        (lambda () (prepare1:collect who stmt close-on-exec?))))

    (define/private (prepare1:collect who stmt close-on-exec?)
      (match (recv-message who)
        [(Result:Prepared _ stmt-id param-dvecs result-dvecs)
         (new prepared-statement%
              (handle stmt-id)
              (close-on-exec? close-on-exec?)
              (param-typeids (map dvec-type param-dvecs))
              (result-dvecs result-dvecs)
              (stmt stmt)
              (owner this))]
        [other (error/comm who "during prepare")]))

    ;; free-statement : prepared-statement -> void
    (define/public (free-statement pst need-lock?)
      ;; protocol does not seem to have any way of freeing statements
      (void))

    ;; ============================================================
    ;; == Query

    ;; query : symbol Statement boolean -> QueryResult
    (define/public (query who stmt cursor?)
      (when cursor? (error 'cassandra:query "cursors not supported yet"))
      (call-with-lock who
        (lambda ()
          (let ([stmt (check-statement who stmt cursor?)])
            (query1 who stmt)))))

    (define/private (query1 who stmt)
      (fresh-exchange)
      (match stmt
        [(statement-binding pst params)
         (buffer-message (Execute (send pst get-handle) consistency params))]
        [(? string? stmt)
         (buffer-message (Query stmt consistency #f))])
      (call-with-sync who (lambda () (query1:collect who stmt))))

    ;; check-statement : symbol statement -> statement
    (define/private (check-statement who stmt cursor?)
      (cond [(statement-binding? stmt)
             (let ([pst (statement-binding-pst stmt)])
               (send pst check-owner who this stmt)
               stmt)]
            [(string? stmt) stmt]))

    (define/private (query1:collect who stmt)
      (match (recv-message who)
        [(Result:Void _)
         (simple-result #f)]
        [(Result:Rows _ _pagestate dvecs rows)
         (rows-result (map dvec->field-info dvecs) rows)]))

    ;; == Cursor

    (define/public (fetch/cursor who cursor fetch-size)
      (error who "not yet implemented"))

    ;; == Internal query

    (define/private (internal-query1 who sql)
      (query1 who sql #f #f))

    ;; == Reflection

    (define/public (list-tables who schema)
      (error who "not yet implemented"))
    ))

;; ========================================

;; raise-backend-error : symbol Error -> raises exn
(define (raise-backend-error who r)
  (match r
    [(Error code message)
     (raise-sql-error who code message `((code . ,code) (message . ,message)))]))
