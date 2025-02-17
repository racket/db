#lang racket/base
(require racket/class
         racket/match
         openssl
         openssl/sha1
         db/private/generic/interfaces
         db/private/generic/common
         db/private/generic/prepared
         db/private/generic/sql-data
         "message.rkt"
         "dbsystem.rkt")
(provide connection%
         mysql-password-hash
         MAX-ALLOWED-PACKET)

(define MAX-ALLOWED-PACKET (expt 2 30))

;; ========================================

(define connection%
  (class* statement-cache% (connection<%>)
    (init-private notice-handler
                  ;; custodian-b : (U #f (custodian-box Boolean))
                  ;; If present, should have same custodian as underlying
                  ;; ports. Useful because SSL obscures port closure.
                  custodian-b)
    (define inport #f)
    (define outport #f)
    (define dbsystem dbsystem:base) ;; updated in connect

    (inherit call-with-lock
             call-with-lock*
             add-delayed-call!
             check-valid-tx-status
             get-tx-status
             set-tx-status!
             check-statement/tx
             dprintf
             prepare1
             check/invalidate-cache)

    (init-field [max-allowed-packet MAX-ALLOWED-PACKET])

    (super-new)

    ;; ========================================

    ;; == Communication

    #|
    During initial setup, okay to send and recv directly, since reference
    to connection does not escape to user. In particular, no danger of trying
    to start a new exchange on top of an incomplete failed one.

    After initial setup, communication can only happen within lock, and any
    error (other than exn:fail:sql) that occurs between sending the message
    buffer (flush-message-buffer) and receiving the last message (recv)
    must cause the connection to disconnect. Such errors include communication
    errors and breaks.
    |#

    ;; msg-buffer : (Listof (cons Message Nat))
    ;; The stored packet number might not be the actual packet number because of
    ;; large-packet splitting (see write-packet). We assume splitting only
    ;; happens for command packets, which get an exchange to themselves, so we
    ;; don't do any checks or adjustments.
    (define msg-buffer null)
    (define next-msg-num 0)

    (define/private (fresh-exchange)
      (set! next-msg-num 0))

    ;; buffer-message : message -> void
    (define/private (buffer-message msg)
      (dprintf (if (can-be-long-packet? msg) "  >> #~s ~.s\n" "  >> #~s ~s\n")
               next-msg-num msg)
      (set! msg-buffer (cons (cons msg next-msg-num) msg-buffer))
      (set! next-msg-num (add1 next-msg-num)))

    ;; flush-message-buffer : -> void
    (define/private (flush-message-buffer)
      (for ([msg+num (in-list (reverse msg-buffer))])
        (write-packet outport (car msg+num) (cdr msg+num)))
      (set! msg-buffer null)
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
                         (unless (exn:fail:sql? e)
                           (disconnect* #f))
                         (raise e))])
        (flush-message-buffer)
        (proc)))

    ;; recv : symbol/#f [(list-of symbol)] -> message
    ;; Automatically handles asynchronous messages
    (define/private (recv fsym expectation [field-dvecs #f])
      (define r (recv* fsym expectation field-dvecs))
      (when (error-packet? r)
        (raise-backend-error fsym r))
      r)

    (define/private (recv* fsym expectation field-dvecs)
      (define (advance . ss)
        (unless (or (not expectation) 
                    (null? ss)
                    (memq expectation ss))
          (error/comm fsym)))
      (define (err packet)
        (error/comm fsym))
      (let-values ([(msg-num next) (parse-packet inport expectation field-dvecs)])
        (set! next-msg-num (add1 msg-num))
        (dprintf (if (can-be-long-packet? next) "  << #~s ~.s\n" "  << #~s ~s\n")
                 msg-num next)
        ;; Update transaction status (see Transactions below)
        (when (ok-packet? next)
          (set-tx-status! fsym (bitwise-bit-set? (ok-packet-server-status next) 0)))
        (when (eof-packet? next)
          (set-tx-status! fsym (bitwise-bit-set? (eof-packet-server-status next) 0)))
        (when (error-packet? next)
          (when (member (error-packet-errno next) '(1213 1205))
            (when (get-tx-status)
              (set-tx-status! fsym 'invalid))))
        (match next
          [(? handshake-packet?)
           (advance 'handshake)]
          [(? ok-packet?)
           (advance)]
          [(? change-plugin-packet?)
           (advance 'auth)]
          [(? error-packet?)
           (advance)]
          [(? result-set-header-packet?)
           (advance 'result)]
          [(? field-packet?)
           (advance 'field)]
          [(? row-data-packet?)
           (advance 'data)]
          [(? binary-row-data-packet?)
           (advance 'binary-data)]
          [(? ok-prepared-statement-packet?)
           (advance 'prep-ok)]
          [(? eof-packet?)
           (advance 'field 'data 'binary-data)]
          [(auth-more-data-packet _)
           (advance)]
          [(struct unknown-packet (expected contents))
           (error/comm fsym expected)]
          [else
           (err next)])
        next))

    ;; ========================================

    ;; Connection management

    (define/override (disconnect* politely?)
      (super disconnect* politely?)
      (let ([outport* outport]
            [inport* inport])
        (when outport*
          (when politely?
            (fresh-exchange)
            (send-message (make-command-packet 'quit "")))
          (with-handlers ([exn:fail? void]) (close-output-port outport*))
          (set! outport #f))
        (when inport*
          (with-handlers ([exn:fail? void]) (close-input-port inport*))
          (set! inport #f))))

    ;; connected? : -> boolean
    (define/override (connected?)
      (let ([outport outport])
        (and outport (not (port-closed? outport))
             (if custodian-b (custodian-box-value custodian-b) #t))))

    (define/public (get-dbsystem)
      dbsystem)

    ;; ========================================

    ;; == Connect

    ;; attach-to-ports : input-port output-port -> void
    (define/public (attach-to-ports in out)
      (set! inport in)
      (set! outport out))

    ;; start-connection-protocol : string/#f string string/#f -> void
    (define/public (start-connection-protocol dbname username password transport
                                              ssl ssl-context hostname allow-cleartext-password?)
      (define using-ssl? #f)  ;; boolean, mutated

      (define (connect:begin)
        (fresh-exchange)
        (let ([r (recv 'mysql-connect 'handshake)])
          (match r
            [(handshake-packet pver sver tid scramble capabilities charset status auth)
             (check-required-flags capabilities)
             (define do-ssl?
               (and (case ssl ((yes optional) #t) ((no) #f))
                    (memq 'ssl capabilities)))
             (when (and (eq? ssl 'yes) (not do-ssl?))
               (error 'mysql-connect "back end refused SSL connection"))
             (unless (member auth '("mysql_native_password" "caching_sha2_password" #f))
               (error* 'mysql-connect "back end requested unsupported authentication plugin"
                       '("plugin" value) auth))
             (define wanted-capabilities (desired-capabilities capabilities do-ssl? dbname))
             (define sver-list (parse-server-version sver))
             (define-values (client-charset set-names?)
               (cond [(memq charset '(utf8mb4_0900_ai_ci utf8mb4_general_ci))
                      (values charset #f)]
                     [(version<=? sver-list '(5 5 2)) ;; < 5.5.3
                      (values 'utf8_general_ci #f)]
                     [else (values 'utf8mb4_general_ci #f)]))
             (set! dbsystem (select-dbsystem sver-list))
             (when do-ssl? (connect:ssl wanted-capabilities))
             (auth-loop (or auth "mysql_native_password") scramble
                        (lambda (auth-plugin data)
                          (make-client-auth-packet wanted-capabilities
                                                   max-allowed-packet
                                                   client-charset
                                                   username data dbname auth-plugin)))
             (after-connect set-names?)]
            [_ (error/comm 'mysql-connect "during authentication")])))

      (define (connect:ssl wanted-capabilities)
        (send-message (make-abbrev-client-auth-packet wanted-capabilities))
        (define-values (sin sout)
          (ports->ssl-ports inport outport
                            #:hostname hostname
                            #:mode 'connect
                            #:context ssl-context
                            #:close-original? #t))
        (attach-to-ports sin sout)
        (set! using-ssl? #t))

      (define (auth-loop auth-plugin scramble make-auth)
        ;; make-auth is a procedure on the first auth exchange, #f on subsequent
        ;; because protocol distinguishes (client-auth-packet vs auth-followup-packet)
        (define (auth data)
          (cond [make-auth (make-auth auth-plugin data)]
                [else (make-auth-followup-packet data)]))
        (cond [(not password)
               (unless make-auth (error/need-password 'mysql-connect))
               (send-message (auth #f))
               (auth:continue scramble)]
              [(equal? auth-plugin "mysql_native_password")
               (send-message (auth (scramble-password scramble password)))
               (auth:continue scramble)]
              [(equal? auth-plugin "mysql_old_password")
               (send-message
                (auth (bytes-append (old-scramble-password scramble password) (bytes 0))))
               (auth:continue scramble)]
              [(equal? auth-plugin "mysql_clear_password")
               ;; Note: untested, since the server-side authentication plugins that use
               ;; mysql_clear_password on the client seem to be only available for commercial
               ;; versions of MySQL.
               (unless (check-allow-cleartext-password?)
                 (error 'mysql-connect "mysql_clear_password authentication failed~a~a"
                        ";\n refusing to send password (see `allow-cleartext-password?`)"))
               (send-message (auth (bytes-append (string->bytes/utf-8 password #"\0"))))
               (auth:continue scramble)]
              [(equal? auth-plugin "caching_sha2_password")
               (send-message (auth (sha256-scramble-password scramble password)))
               (auth:continue-caching-sha2 scramble)]
              [else
               (error 'mysql-connect "back end requested unsupported authentication plugin\n  plugin: ~v"
                      auth-plugin)]))

      (define (auth:continue scramble #:message [msg #f])
        (match (or msg (recv 'mysql-connect 'auth))
          [(ok-packet _ _ status warnings message)
           (void)]
          [(change-plugin-packet plugin data)
           ;; if plugin = #f, means "mysql_old_password"
           ;; data may contain more than just scramble/nonce (eg, for
           ;; mysql_native_password on MySQL 8.0.16, data has extra "\0" at end; the
           ;; {scramble,old-scramble}-password function trim to appropriate prefix.
           (auth-loop (or plugin "mysql_old_password") (or data scramble) #f)]))

      (define (auth:continue-caching-sha2 scramble)
        (match (recv 'mysql-connect 'auth)
          [(auth-more-data-packet #"\3") ;; fast_auth_success
           (auth:continue scramble)]
          [(auth-more-data-packet #"\4") ;; perform_full_authentication
           (cond [(and (eq? transport 'tcp) (not using-ssl?))
                  ;; The caching_sha2_password protocol says we should now
                  ;; - (optionally) request the server's RSA public key, and then
                  ;; - send the password encrypted with the public key (directly?!)
                  ;; From the docs, it sounds like the public key is sent without a
                  ;; certificate---that is, unauthenticated! So what's the point?
                  ;; FIXME: add option to use existing (presumably trusted) public key?
                  (error 'mysql-connect "caching_sha2_password authentication failed~a~a"
                         ";\n slow path not supported for TCP without TLS"
                         ";\n and the server rejected the fast path")]
                 [else ;; unix domain socket or TCP with TLS => "secure", don't encrypt password
                  (unless (check-allow-cleartext-password?)
                    (error 'mysql-connect "caching_sha2_password authentication failed~a~a"
                           ";\n refusing to send password (see `allow-cleartext-password?`)"
                           ";\n and the server rejected the fast path"))
                  (send-message
                   (auth-followup-packet (bytes-append (string->bytes/utf-8 password) #"\0")))
                  (auth:continue scramble)])]
          [(? change-plugin-packet? msg)
           (auth:continue scramble #:message msg)]
          [else (error/comm 'mysql-connect "during authentication (caching_sha2_password)")]))

      (define (check-allow-cleartext-password?)
        (or (eq? allow-cleartext-password? #t)
            (and (eq? allow-cleartext-password? 'local)
                 (or (eq? transport 'socket)
                     (equal? hostname "localhost")))))

      (connect:begin))

    (define/private (check-required-flags capabilities)
      (for-each (lambda (rf)
                  (unless (memq rf capabilities)
                    (error* 'mysql-connect "server does not support required capability"
                            "capability" rf)))
                REQUIRED-CAPABILITIES))

    (define/private (desired-capabilities capabilities ssl? dbname)
      (append (if ssl?   '(ssl)             '())
              (if dbname '(connect-with-db) '())
              '(interactive)
              (filter (lambda (c) (memq c DESIRED-CAPABILITIES)) capabilities)))

    (define/private (after-connect set-names?)
      (when set-names?
        (with-handlers ([exn:fail:sql?
                         (lambda (e) (dprintf "  !! failed SET NAMES: ~e\n" (exn-message e)))])
          (query 'mysql-connect "SET NAMES 'utf8mb4'" #f))))

    ;; ========================================

    ;; == Query

    ;; query : symbol Statement boolean -> QueryResult
    (define/public (query fsym stmt cursor?)
      (let ([result
             (call-with-lock fsym
               (lambda ()
                 (check-valid-tx-status fsym)
                 (let* ([stmt (check-statement fsym stmt cursor?)]
                        [stmt-type
                         (cond [(statement-binding? stmt)
                                (send (statement-binding-pst stmt) get-stmt-type)]
                               [(string? stmt)
                                (classify-my-sql stmt)])])
                   (check-statement/tx fsym stmt-type)
                   (begin0 (query1 fsym stmt cursor? #t)
                     (statement:after-exec stmt #f)))))])
        (query1:process-result fsym result)))

    ;; query1 : symbol Statement -> QueryResult
    (define/private (query1 fsym stmt cursor? warnings?)
      (let ([delenda (check/invalidate-cache stmt)])
        (when delenda
          (for ([(_sql pst) (in-hash delenda)])
            (free-statement pst #f))))
      (let ([wbox (and warnings? (box 0))])
        (fresh-exchange)
        (query1:enqueue fsym stmt cursor?)
        (begin0 (call-with-sync fsym
                  (lambda () (query1:collect fsym stmt (not (string? stmt)) cursor? wbox)))
          (when (and warnings? (not (zero? (unbox wbox))))
            (fetch-warnings fsym)))))

    ;; check-statement : symbol any boolean -> statement-binding
    ;; For cursor, need to clone pstmt, because only one cursor can be
    ;; open for a statement at a time. (Could delay clone until
    ;; needed, but that seems more complicated.)
    (define/private (check-statement fsym stmt cursor?)
      (cond [(statement-binding? stmt)
             (let ([pst (statement-binding-pst stmt)])
               (send pst check-owner fsym this stmt)
               (for ([typeid (in-list (send pst get-result-typeids))])
                 (unless (supported-result-typeid? typeid)
                   (error/unsupported-type fsym typeid)))
               (cond [cursor?
                      (let ([pst* (prepare1 fsym (send pst get-stmt) #f)])
                        (statement-binding pst* (statement-binding-params stmt)))]
                     [else stmt]))]
            [(and (string? stmt) (force-prepare-sql? fsym stmt))
             (let ([pst (prepare1 fsym stmt (not cursor?))])
               (check-statement fsym (send pst bind fsym null) #f))]
            [else stmt]))

    ;; query1:enqueue : Symbol Statement -> void
    (define/private (query1:enqueue fsym stmt cursor?)
      (cond [(statement-binding? stmt)
             (let* ([pst (statement-binding-pst stmt)]
                    [id (send pst get-handle)]
                    [params (map (lambda (p) (if (sql-null? p) '(null . #f) p))
                                 (statement-binding-params stmt))]
                    [param-count (length params)]
                    [null-map (for/list ([p (in-list params)]) (eq? (car p) 'null))]
                    [flags (if cursor? '(cursor/read-only) '())])
               ;; params is (Listof CheckedParam-v1)

               ;; Ideally, we would just generate a single execute packet with all param values.
               ;; Unfortunately, a fragmented execute packet (see write-packet) seems to result
               ;; in corrupted data (even if max_allowed_packet is larger). So try to send
               ;; large params as send-long-data packets, if possible.
               (define MAX-SIZE (- (min max-allowed-packet MAX-PAYLOAD) 20))

               ;; Oversimplified packet size estimate:
               ;;   - 20 bytes overhead for other packet fields
               ;;   - binary-datum-size of params
               (define (param-size p) (binary-datum-size (car p) (cdr p)))
               ;; Only certain types (var-string and blob) can be sent as long-data.
               (define (can-long? p) (and (memq (car p) '(var-string blob)) #t))
               (define size0 (for/sum ([p (in-list params)]) (param-size p)))
               (cond [(< size0 MAX-SIZE)
                      (buffer-message (make-execute-packet id flags null-map params))]
                     [else
                      (define long-opps ;; (Listof (list* Nat Nat CPv1)), sorted biggest first
                        (sort (for/list ([p (in-list params)] [param-id (in-naturals)]
                                         #:when (can-long? p))
                                (list* (param-size p) param-id p))
                              > #:key car))
                      (define long-params ;; Hash[Nat => CPv1]
                        (let loop ([long-opps long-opps] [size size0])
                          (cond [(< size MAX-SIZE)
                                 #hash()]
                                [(null? long-opps)
                                 (error/params/max-packet fsym size0 MAX-SIZE max-allowed-packet)]
                                [else
                                 (define opp (car long-opps))
                                 (hash-set (loop (cdr long-opps) (- size (car opp)))
                                           (cadr opp) (cddr opp))])))
                      (for ([(long-param-id long-param) (in-hash long-params)])
                        (query1:send-long-data-param id long-param-id (cdr long-param)))
                      (define short-params ;; (Listof CheckedParam-v2)
                        (for/list ([p (in-list params)] [param-id (in-naturals)])
                          (if (hash-ref long-params param-id #f) (cons (car p) #f) p)))
                      (buffer-message (make-execute-packet id flags null-map short-params))]))]
            [else ;; string
             (buffer-message (make-command-packet 'query stmt))]))

    ;; query1:send-long-data-param : Nat Nat Bytes -> Void
    (define/private (query1:send-long-data-param id param-id param)
      (define pblen (bytes-length param))
      (define CHUNK (- max-allowed-packet 10))
      (let chunkloop ([start 0])
        (when (< start pblen)
          (let ([next (min pblen (+ start CHUNK))])
            (buffer-message (make-long-data-packet id param-id (subbytes param start next)))
            (fresh-exchange)
            (chunkloop next)))))

    ;; query1:collect : symbol bool -> QueryResult stream
    (define/private (query1:collect fsym stmt binary? cursor? wbox)
      (let ([r (recv fsym 'result)])
        (match r
          [(struct ok-packet (affected-rows insert-id status warnings message))
           (when wbox (set-box! wbox warnings))
           (vector 'command `((affected-rows . ,affected-rows)
                              (insert-id . ,(if (zero? insert-id) #f insert-id))
                              (status . ,status)
                              (message . ,message)))]
          [(struct result-set-header-packet (fields extra))
           (define mbox (box #f)) ;; more resultsets?
           (define field-dvecs (query1:get-fields fsym mbox))
           (cond [cursor?
                  (vector 'cursor field-dvecs (statement-binding-pst stmt))]
                 [else
                  (define rows (query1:get-rows fsym field-dvecs binary? wbox #f mbox))
                  (define result (vector 'rows field-dvecs rows))
                  (cond [(unbox mbox)
                         (cons result (query1:collect fsym stmt binary? cursor? wbox))]
                        [else result])])])))

    (define/private (query1:get-fields fsym mbox)
      (let ([r (recv fsym 'field)])
        (match r
          [(? field-packet?)
           (cons (parse-field-dvec r) (query1:get-fields fsym mbox))]
          [(struct eof-packet (warning status))
           (when (and mbox (bitwise-bit-set? status MORE-RESULTS-EXIST-BIT))
             (set-box! mbox #t))
           null])))

    (define/private (query1:get-rows fsym field-dvecs binary? wbox end-box mbox)
      ;; Note: binary? should always be #t, unless force-prepare-sql? misses something.
      (let ([r (recv fsym (if binary? 'binary-data 'data) field-dvecs)])
        (match r
          [(struct row-data-packet (data))
           (cons data (query1:get-rows fsym field-dvecs binary? wbox end-box mbox))]
          [(struct binary-row-data-packet (data))
           (cons data (query1:get-rows fsym field-dvecs binary? wbox end-box mbox))]
          [(struct eof-packet (warnings status))
           (when wbox (set-box! wbox warnings))
           (when (and end-box (bitwise-bit-set? status LAST-ROW-SENT-BIT))
             (set-box! end-box #t))
           (when (and mbox (bitwise-bit-set? status MORE-RESULTS-EXIST-BIT))
             (set-box! mbox #t))
           null])))

    (define/private (query1:process-result fsym result)
      (match result
        [(vector 'rows field-dvecs rows)
         (rows-result (map field-dvec->field-info field-dvecs) rows)]
        [(vector 'command command-info)
         (simple-result command-info)]
        [(vector 'cursor field-dvecs pst)
         (cursor-result (map field-dvec->field-info field-dvecs)
                        pst
                        (list field-dvecs (box #f)))]
        [(cons result1 (vector 'command _))
         (query1:process-result fsym result1)]
        [(cons _ _)
         (error fsym "multiple result sets not allowed")]))

    ;; == Cursor

    (define/public (fetch/cursor fsym cursor fetch-size)
      (let ([pst (cursor-result-pst cursor)]
            [extra (cursor-result-extra cursor)])
        (send pst check-owner fsym this pst)
        (let ([field-dvecs (car extra)]
              [end-box (cadr extra)])
          (call-with-lock fsym
            (lambda ()
              (cond [(unbox end-box)
                     #f]
                    [else
                     (let ([wbox (box 0)])
                       (fresh-exchange)
                       (buffer-message (make-fetch-packet (send pst get-handle) fetch-size))
                       (begin0 (call-with-sync fsym
                                 (lambda () (query1:get-rows fsym field-dvecs #t wbox end-box #f)))
                         (when (not (zero? (unbox wbox)))
                           (fetch-warnings fsym))))]))))))

    ;; == Prepare

    (define/override (classify-stmt sql) (classify-my-sql sql))

    (define/override (prepare1* fsym stmt close-on-exec? stmt-type)
      (fresh-exchange)
      (buffer-message (make-command-packet 'statement-prepare stmt))
      (call-with-sync fsym
        (lambda ()
          (let ([r (recv fsym 'prep-ok)])
            (match r
              [(struct ok-prepared-statement-packet (id fields params))
               (let ([param-dvecs
                      (if (zero? params) null (prepare1:get-field-descriptions fsym))]
                     [field-dvecs
                      (if (zero? fields) null (prepare1:get-field-descriptions fsym))])
                 (new prepared-statement%
                      (handle id)
                      (close-on-exec? close-on-exec?)
                      (param-typeids (map field-dvec->typeid param-dvecs))
                      (result-dvecs field-dvecs)
                      (stmt stmt)
                      (stmt-type stmt-type)
                      (owner this)))])))))

    (define/private (prepare1:get-field-descriptions fsym)
      (let ([r (recv fsym 'field)])
        (match r
          [(struct eof-packet (warning-count status))
           null]
          [(? field-packet?)
           (cons (parse-field-dvec r) (prepare1:get-field-descriptions fsym))])))

    (define/public (get-base) this)

    (define/public (free-statement pst need-lock?)
      ;; Important: *buffer* statement-close message, but do not send (ie, flush).
      ;; That way, message included in same TCP packet as next query message, avoiding
      ;; write-write-read TCP packet sequence, Nagle's algorithm & delayed ACK issue.
      (define (do-free-statement)
        (let ([id (send pst get-handle)])
          (when (and id outport) ;; outport = connected?
            (send pst set-handle #f)
            (fresh-exchange)
            (buffer-message (make-command:statement-packet 'statement-close id)))))
      (if need-lock?
          (call-with-lock* 'free-statement do-free-statement void #f)
          (do-free-statement)))

    ;; == Warnings

    (define/private (fetch-warnings fsym)
      (unless (eq? notice-handler void)
        (let ([result (query1 fsym "SHOW WARNINGS" #f #f)])
          (define (find-index name dvecs)
            (for/or ([dvec (in-list dvecs)]
                     [i (in-naturals)])
              (and (equal? (field-dvec->name dvec) name) i)))
          (match result
            [(vector 'rows field-dvecs rows)
             (let ([code-index (find-index "Code" field-dvecs)]
                   [message-index (find-index "Message" field-dvecs)])
               (for ([row (in-list rows)])
                 (let ([code (string->number (vector-ref row code-index))]
                       [message (vector-ref row message-index)])
                   (add-delayed-call! (lambda () (notice-handler code message))))))]))))

    ;; == Transactions

    ;; MySQL: what causes implicit commit, when is transaction rolled back
    ;;   http://dev.mysql.com/doc/refman/5.1/en/implicit-commit.html
    ;;   http://dev.mysql.com/doc/refman/5.1/en/innodb-error-handling.html
    ;;   http://dev.mysql.com/doc/refman/5.1/en/innodb-error-codes.html
    ;;
    ;; Sounds like MySQL rolls back transaction (but may keep open!) on
    ;;   - transaction deadlock = 1213 (ER_LOCK_DEADLOCK)
    ;;   - lock wait timeout (depends on config) = 1205 (ER_LOCK_WAIT_TIMEOUT)

    (define/override (start-transaction* fsym isolation option)
      (cond [(eq? isolation 'nested)
             (let ([savepoint (generate-name)])
               (query1 fsym (format "SAVEPOINT ~a" savepoint) #f #t)
               savepoint)]
            [else
             (let ([isolation-level (isolation-symbol->string isolation)])
               (when option
                 ;; No options supported
                 (raise-argument-error fsym "#f" option))
               (when isolation-level
                 (query1 fsym (format "SET TRANSACTION ISOLATION LEVEL ~a" isolation-level) #f #t))
               (query1 fsym "START TRANSACTION" #f #t)
               #f)]))

    (define/override (end-transaction* fsym mode savepoint)
      (case mode
        ((commit)
         (cond [savepoint
                (query1 fsym (format "RELEASE SAVEPOINT ~a" savepoint) #f #t)]
               [else
                (query1 fsym "COMMIT" #f #t)]))
        ((rollback)
         (cond [savepoint
                (query1 fsym (format "ROLLBACK TO SAVEPOINT ~a" savepoint) #f #t)
                (query1 fsym (format "RELEASE SAVEPOINT ~a" savepoint) #f #t)]
               [else
                (query1 fsym "ROLLBACK" #f #t)])))
      (void))

    ;; name-counter : number
    (define name-counter 0)

    ;; generate-name : -> string
    (define/private (generate-name)
      (let ([n name-counter])
        (set! name-counter (add1 name-counter))
        (format "λmz_~a" n)))

    ;; Reflection

    (define/public (list-tables fsym schema)
      (let* ([stmt
              ;; schema is ignored; search = current
              (string-append "SELECT table_name FROM information_schema.tables "
                             "WHERE table_schema = schema()")]
             [rows
              (vector-ref (call-with-lock fsym (lambda () (query1 fsym stmt #f #t))) 2)])
        (for/list ([row (in-list rows)])
          (vector-ref row 0))))

    ))

;; ========================================

;; mysql-password-hash : string -> string
(define (mysql-password-hash password)
  (bytes->hex-string (password-hash password)))

;; scramble-password : bytes string -> bytes
(define (scramble-password scramble password)
  (and scramble password
       (let* ([scramble (subbytes scramble 0 20)]
              [stage1 (cond [(string? password) (password-hash password)]
                            [(pair? password) (hex-string->bytes (cadr password))])]
              [stage2 (sha1-bytes (open-input-bytes stage1))]
              [stage3 (sha1-bytes (open-input-bytes (bytes-append scramble stage2)))]
              [reply (bytes-xor stage1 stage3)])
         reply)))

;; password-hash : string -> bytes
(define (password-hash password)
  (let* ([password (string->bytes/latin-1 password)]
         [stage1 (sha1-bytes (open-input-bytes password))])
    stage1))

;; bytes-xor : bytes bytes -> bytes
;; Assumes args are same length
(define (bytes-xor a b)
  (let ([c (make-bytes (bytes-length a))])
    (let loop ([i 0])
      (when (< i (bytes-length c))
        (bytes-set! c i
                    (bitwise-xor (bytes-ref a i) (bytes-ref b i)))
        (loop (add1 i))))
    c))

;; =======================================

(provide sha256-scramble-password)

(define (sha256-scramble-password scramble password-str)
  (let ([scramble (subbytes scramble 0 20)])
    (define password (string->bytes/utf-8 password-str))
    (define password-h (sha256-bytes password))
    (define password-hh (sha256-bytes password-h))
    (bytes-xor password-h (sha256-bytes (bytes-append password-hh scramble)))))

;; =======================================

(provide old-scramble-password
         hash323
         hash323->string)

(define (old-scramble-password scramble password)
  (define (xor a b) (bitwise-xor a b))
  (define RMAX #x3FFFFFFF)
  (and scramble password
       (let* ([scramble (subbytes scramble 0 8)]
              [password (string->bytes/utf-8 password)]
              [hp (hash323 password)]
              [hm (hash323 scramble)]
              [r1 (modulo (xor (car hp) (car hm)) RMAX)]
              [r2 (modulo (xor (cdr hp) (cdr hm)) RMAX)]
              [out (make-bytes 8 0)])
         (define (rnd)
           (set! r1 (modulo (+ (* 3 r1) r2) RMAX))
           (set! r2 (modulo (+ r1 r2 33) RMAX))
           (/ (exact->inexact r1) (exact->inexact RMAX)))
         (for ([i (in-range (bytes-length scramble))])
           (let ([b (+ (inexact->exact (floor (* (rnd) 31))) 64)])
             (bytes-set! out i b)
             (values r1 r2)))
         (let ([extra (inexact->exact (floor (* (rnd) 31)))])
           (for ([i (in-range (bytes-length scramble))])
             (bytes-set! out i (xor (bytes-ref out i) extra))))
         out)))

(define (hash323 bs)
  (define (xor a b) (bitwise-xor a b))
  (define-syntax-rule (normalize! var)
    (set! var (bitwise-and var (sub1 (arithmetic-shift 1 64)))))
  (let ([nr 1345345333]
        [add 7]
        [nr2 #x12345671])
    (for ([i (in-range (bytes-length bs))]
          #:when (not (memv (bytes-ref bs i) '(#\space #\tab))))
      (let ([tmp (bytes-ref bs i)])
        (set! nr  (xor nr
                       (+ (* (+ (bitwise-and nr 63) add) tmp)
                          (arithmetic-shift nr 8))))
        (normalize! nr)
        (set! nr2 (+ nr2
                     (xor (arithmetic-shift nr2 8) nr)))
        (normalize! nr2)
        (set! add (+ add tmp))
        (normalize! add)))
    (cons (bitwise-and nr  (sub1 (arithmetic-shift 1 31)))
          (bitwise-and nr2 (sub1 (arithmetic-shift 1 31))))))

(define (hash323->string bs)
  (let ([p (hash323 bs)])
    (bytes-append (integer->integer-bytes (car p) 4 #f #f)
                  (integer->integer-bytes (cdr p) 4 #f #f))))

;; ========================================

(define REQUIRED-CAPABILITIES
  '(long-flag
    connect-with-db
    protocol-41
    secure-connection))

(define DESIRED-CAPABILITIES
  '(long-password
    long-flag
    transactions
    protocol-41
    secure-connection
    plugin-auth
    multi-results
    ps-multi-results))

;; raise-backend-error : symbol ErrorPacket -> raises exn
(define (raise-backend-error who r)
  (define code (error-packet-sqlstate r))
  (define message (error-packet-message r))
  (define props (list (cons 'errno (error-packet-errno r))
                      (cons 'code code)
                      (cons 'message message)))
  (raise-sql-error who code message props))

(define (error/params/max-packet fsym size MAX-SIZE max-allowed-packet)
  (error fsym "parameters excluding TEXT and BLOB are too large (protocol limit)~a"
         (cond [(> size MAX-SIZE)
                ";\n parameters exceed execute-packet payload size"]
               [else
                (format ";\n ~a\n  max-allowed-packet: ~s"
                        "parameters exceed client max-allowed-packet parameter"
                        max-allowed-packet)])))

;; ========================================

#|
MySQL allows only certain kinds of statements to be prepared; the rest
must go through the old execution path. See here:
  http://dev.mysql.com/doc/refman/5.0/en/c-api-prepared-statements.html
According to that page, the following statements may be prepared:

  CALL, CREATE TABLE, DELETE, DO, INSERT, REPLACE, SELECT, SET, UPDATE,
  and most SHOW statements

On the other hand, we want to force all rows-returning statements
through the prepared-statement path to use the binary data
protocol. That would seem to be the following:

  SELECT and SHOW
|#

(define (force-prepare-sql? fsym stmt)
  (memq (classify-my-sql stmt) '(select show)))
