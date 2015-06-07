#lang racket/base
(require racket/class
         syntax/parse/private/minimatch
         racket/place
         racket/promise
         racket/serialize
         racket/runtime-path
         (for-syntax (only-in racket/base quote))
         ffi/unsafe/atomic
         db/private/generic/interfaces
         db/private/generic/common
         db/private/generic/prepared)
(provide place-connect
         place-proxy-connection%)

(define (pchan-put chan datum)
  (place-channel-put chan (serialize datum)))
(define (pchan-get chan)
  (deserialize (place-channel-get chan)))

(define-runtime-module-path-index _place-server
  'db/private/generic/place-server)

(define connection-server-channel
  (delay/sync
   (dynamic-place 'db/private/generic/place-server 'connection-server)))

(define (place-connect connection-spec proxy%)
  (let-values ([(channel other-channel) (place-channel)])
    (place-channel-put (force connection-server-channel)
                       (list 'connect other-channel connection-spec))
    (match (pchan-get channel)
      [(list 'ok)
       (new proxy% (channel channel))]
      [(list 'error message)
       (raise (make-exn:fail message (current-continuation-marks)))])))

(define place-proxy-connection%
  (class* locking% (connection<%>)
    (init channel)
    (field [channel-box (make-custodian-box (current-custodian) channel)])
    (inherit call-with-lock
             call-with-lock*)
    (super-new)

    (define/private (call method-name who . args)
      (call-with-lock who (lambda () (call* method-name who args #t))))
    (define/private (call/d method-name who . args)
      (call-with-lock* who (lambda () (call* method-name who args #f)) #f #f))
    (define/private (call* method-name who args need-connected?)
      (cond [(and channel-box (custodian-box-value channel-box))
             => (lambda (channel)
                  (pchan-put channel (cons method-name args))
                  (let* ([response (pchan-get channel)]
                         [still-connected? (car response)])
                    (when (not still-connected?) (set! channel-box #f))
                    (match (cdr response)
                      [(cons 'values vals)
                       (apply values (for/list ([val (in-list vals)]) (sexpr->result val)))]
                      [(list 'error message)
                       (raise (make-exn:fail message (current-continuation-marks)))])))]
            [need-connected?
             (error/not-connected who)]
            [else (void)]))

    (define/override (connected?)
      (let ([channel-box channel-box])
        (and channel-box (custodian-box-value channel-box) #t)))

    (define/public (disconnect)
      (call/d 'disconnect 'disconnect)
      (set! channel-box #f))

    (define/public (get-dbsystem) (error 'get-dbsystem "not implemented"))
    (define/public (get-base) this)

    (define/public (query who stmt cursor?)
      (call 'query who who
            (match stmt
              [(? string?) (list 'string stmt)]
              [(statement-binding pst params)
               (list 'statement-binding (send pst get-handle) params)])
            cursor?))
    (define/public (prepare who stmt close-on-exec?)
      (call 'prepare who who stmt close-on-exec?))
    (define/public (fetch/cursor who cursor fetch-size)
      (call 'fetch/cursor who who (cursor-result-extra cursor) fetch-size))
    (define/public (transaction-status who)
      (call 'transaction-status who who))
    (define/public (start-transaction who iso option cwt?)
      (call 'start-transaction who who iso option cwt?))
    (define/public (end-transaction who mode cwt?)
      (call 'end-transaction who who mode cwt?))
    (define/public (list-tables who schema)
      (call 'list-tables who who schema))

    (define/public (free-statement pst need-lock?)
      (start-atomic)
      (let ([handle (send pst get-handle)])
        (send pst set-handle #f)
        (end-atomic)
        (when channel-box
          (call/d 'free-statement 'free-statement handle need-lock?))))

    (define/private (sexpr->result x)
      (match x
        [(list 'simple-result y)
         (simple-result y)]
        [(list 'rows-result h rows)
         (rows-result h rows)]
        [(list 'cursor-result info handle)
         (cursor-result info #f handle)]
        [(list 'prepared-statement handle close-on-exec? param-typeids result-dvecs)
         (new prepared-statement%
              (handle handle)
              (close-on-exec? close-on-exec?)
              (param-typeids param-typeids)
              (result-dvecs result-dvecs)
              (owner this))]
        [_ x]))))
