#lang racket/base
(require (for-syntax racket/base)
         racket/string
         racket/port
         racket/match
         db/private/generic/interfaces
         db/private/generic/sql-data)
(provide write-message
         parse-server-message

         (struct-out AuthenticationOk)
         (struct-out AuthenticationKerberosV5)
         (struct-out AuthenticationCleartextPassword)
         (struct-out AuthenticationCryptPassword)
         (struct-out AuthenticationMD5Password)
         (struct-out AuthenticationSCMCredential)
         (struct-out AuthenticationSASL)
         (struct-out AuthenticationSASLContinue)
         (struct-out AuthenticationSASLFinal)
         (struct-out StartupMessage)
         (struct-out SSLRequest)
         (struct-out CancelRequest)
         (struct-out ErrorResponse)
         (struct-out NoticeResponse)
         (struct-out BackendKeyData)
         (struct-out Bind)
         (struct-out BindComplete)
         (struct-out Close)
         (struct-out CloseComplete)
         (struct-out CommandComplete)
         (struct-out CopyInResponse)
         (struct-out CopyOutResponse)
         (struct-out DataRow)
         (struct-out Describe)
         (struct-out EmptyQueryResponse)
         (struct-out Execute)
         (struct-out Flush)
         (struct-out NegotiateProtocolVersion)
         (struct-out NoData)
         (struct-out NotificationResponse)
         (struct-out ParameterDescription)
         (struct-out ParameterStatus)
         (struct-out Parse)
         (struct-out ParseComplete)
         (struct-out PasswordMessage)
         (struct-out PortalSuspended)
         (struct-out Query)
         (struct-out ReadyForQuery)
         (struct-out RowDescription)
         (struct-out SASLInitialResponse)
         (struct-out SASLResponse)
         (struct-out Sync)
         (struct-out Terminate)
         bytes->row
         field-dvec->typeid
         field-dvec->field-info)

;; subport : input-port num -> input-port
;; Reads len bytes from input, then returns input port
;; containing only those bytes.
;; Raises error if fewer than len bytes available in input.
(define (subport in len)
  (let ([bytes (io:read-bytes-as-bytes in len)])
    (unless (and (bytes? bytes) (= (bytes-length bytes) len))
      (error/internal* 'subport "truncated input; got fewer bytes than expected"
                       "expected" len
                       "got" (if (bytes? bytes) (bytes-length bytes) 0)))
    (open-input-bytes bytes)))


;; Integers are signed, network order (big-endian).

;; WRITING FUNCTIONS

;; write-int16 : port integer -> (void)
;; Writes a 16-bit integer, network byte order
(define (io:write-int16 port val)
  (write-bytes (integer->integer-bytes val 2 #t #t) port))

;; write-uint16 : port integer -> (void)
;; Writes an unsigned 16-bit integer, network byte order
(define (io:write-uint16 port val)
  (write-bytes (integer->integer-bytes val 2 #f #t) port))

;; write-int32 : port integer -> void
;; Writes a 32-bit integer, network byte order
(define (io:write-int32 port val)
  (write-bytes (integer->integer-bytes val 4 #t #t) port))

;; write-byte : port byte -> void
(define (io:write-byte port byte)
  (write-byte byte port))

;; write-byte/char : port char -> void
(define (io:write-byte/char port char)
  (write-byte (char->integer char) port))

;; write-bytes : port bytes -> void
(define (io:write-bytes port bytes)
  (write-bytes bytes port))

;; write-null-terminated-string : port string -> void
(define (io:write-null-terminated-string port string)
  (write-string string port)
  (write-byte 0 port))

;; READING

;; read-int16 : port -> integer
(define (io:read-int16 port)
  (integer-bytes->integer (read-bytes 2 port) #t #t))

;; read-uint16 : port -> integer
(define (io:read-uint16 port)
  (integer-bytes->integer (read-bytes 2 port) #f #t))

;; read-int32 : port -> integer
(define (io:read-int32 port)
  (integer-bytes->integer (read-bytes 4 port) #t #t))

;; read-null-terminated-string : port -> string
(define (io:read-null-terminated-string port)
  (let [(strport (open-output-bytes))]
    (let loop ()
      (let ([next (read-byte port)])
        (cond [(zero? next)
               (get-output-string strport)]
              [else
               (write-byte next strport)
               (loop)])))))

;; read-byte : port -> byte
(define (io:read-byte port)
  (read-byte port))

;; read-byte : port-> char
(define (io:read-byte/char port)
  (integer->char (read-byte port)))

;; read-bytes-as-bytes : port number -> bytes
(define (io:read-bytes-as-bytes port n)
  (read-bytes n port))

;; ========================================

(define-syntax-rule (with-length-in p c . body)
  ;; header char read & discarded elsewhere
  (let ([len (io:read-int32 p)])
    (let ([p (subport p (- len 4))])
      . body)))

(define-syntax-rule (with-length-out p c . body)
  (let ([bs (let ([p (open-output-bytes)])
              (let () (begin . body) (void))
              (get-output-bytes p))])
    (when c (io:write-byte/char p c))
    (io:write-int32 p (+ 4 (bytes-length bs)))
    (write-bytes bs p)))

;; ========================================
;; The strange structures

(define PROTO_3_0 #x00030000)
(define PROTO_3_2 #x00030002)

(define PROTO_CANCEL 80877102) ;; 1234<<16 + 5678
(define PROTO_SSL    80877103) ;; 1234<<16 + 5679

(define-struct AuthenticationOk () #:transparent)
(define-struct AuthenticationKerberosV5 () #:transparent)
(define-struct AuthenticationCleartextPassword () #:transparent)
(define-struct AuthenticationCryptPassword (salt) #:transparent)
(define-struct AuthenticationMD5Password (salt) #:transparent)
(define-struct AuthenticationSCMCredential () #:transparent)
(define-struct AuthenticationSASL (methods) #:transparent)
(define-struct AuthenticationSASLContinue (content) #:transparent)
(define-struct AuthenticationSASLFinal (content) #:transparent)
(define (parse:Authentication p)
  (with-length-in p #\R
    (let* ([tag (io:read-int32 p)])
      (case tag
        ((0) (make-AuthenticationOk))
        ((2) (make-AuthenticationKerberosV5))
        ((3) (make-AuthenticationCleartextPassword))
        ((4) (let ([salt (io:read-bytes-as-bytes p 2)])
               (make-AuthenticationCryptPassword salt)))
        ((5) (let ([salt (io:read-bytes-as-bytes p 4)])
               (make-AuthenticationMD5Password salt)))
        ((6) (make-AuthenticationSCMCredential))
        ((10) (let ([methods (io:read-null-terminated-string p)])
                (make-AuthenticationSASL
                 ;; As of PostrgreSQL v10.1 only one method supported, and I
                 ;; can't find docs for how multiple methods might be formatted
                 ;; in future.  But I don't want old clients to break when new
                 ;; method added.  So split on any char illegal in method name.
                 (string-split methods #rx"[^-_A-Z0-9]+"))))
        ((11) (make-AuthenticationSASLContinue (port->string p)))
        ((12) (make-AuthenticationSASLFinal (port->string p)))
        (else
         (error/internal* 'authentication "back end requested unknown authentication method"
                          "method code" tag))))))

(define-struct StartupMessage (parameters) #:transparent)
(define (write:StartupMessage p v)
  (with-length-out p #f
    (io:write-int32 p PROTO_3_0)
    (for-each (lambda (param)
                (io:write-null-terminated-string p (car param))
                (io:write-null-terminated-string p (cdr param)))
              (StartupMessage-parameters v))
    (io:write-byte p 0)))

(define-struct SSLRequest () #:transparent)
(define (write:SSLRequest p v)
  (io:write-int32 p 8)
  (io:write-int32 p PROTO_SSL))

(define-struct CancelRequest (process-id secret-key) #:transparent)
(define (write:CancelRequest p v)
  (io:write-int32 p 16)
  (io:write-int32 p PROTO_CANCEL)
  (io:write-int32 p (CancelRequest-process-id v))
  (write-bytes (CancelRequest-secret-key v) p))

(define-struct ErrorResponse (properties) #:transparent)
(define (parse:ErrorResponse p)
  (with-length-in p #\E
    (let* ([fields (parse-field-list p)])
      (make-ErrorResponse fields))))

(define-struct NoticeResponse (properties) #:transparent)
(define (parse:NoticeResponse p)
  (with-length-in p #\N
    (let* ([fields (parse-field-list p)])
      (make-NoticeResponse fields))))

(define (parse-field-list p)
  (let loop ()
    (let ([next (peek-byte p)])
      (cond [(zero? next)
             (begin (read-byte p) null)]
            [else
             (let* ([tag (integer->char (io:read-byte p))]
                    [value (io:read-null-terminated-string p)])
               (cons (cons (char->message-tag tag) value)
                     (loop)))]))))

;; ========================================
;; The normal structures

(define-struct BackendKeyData (process-id secret-key) #:transparent)
(define (parse:BackendKeyData p)
  (with-length-in p #\K
    (let* ([process-id (io:read-int32 p)]
           ;; Before protocol v3.2 (PostgreSQL 18), secret-key is int32;
           ;; after, var-length up to 256 bytes. So just read to EOM, keep as bytes.
           [secret-key (read-bytes 256 p)])
      (make-BackendKeyData process-id secret-key))))

(define-struct Bind (portal statement param-formats values result-formats) #:transparent)
(define (write:Bind p v)
  (match v
    [(struct Bind (portal statement param-formats values result-formats))
     (with-length-out p #\B
       (io:write-null-terminated-string p portal)
       (io:write-null-terminated-string p statement)
       (io:write-uint16 p (length param-formats))
       (for ([param-format (in-list param-formats)])
         (io:write-int16 p param-format))
       (io:write-uint16 p (length values))
       (for ([value (in-list values)])
         (cond [(bytes? value)
                (io:write-int32 p (bytes-length value))
                (io:write-bytes p value)]
               [(string? value)
                (let ([value (string->bytes/utf-8 value)])
                  (io:write-int32 p (bytes-length value))
                  (io:write-bytes p value))]
               [(sql-null? value)
                (io:write-int32 p -1)]))
       (io:write-uint16 p (length result-formats))
       (for ([result-format (in-list result-formats)])
         (io:write-int16 p result-format)))]))

(define-struct BindComplete () #:transparent)
(define (parse:BindComplete p)
  (with-length-in p #\2
    (make-BindComplete)))

(define-struct Close (type name) #:transparent)
(define (write:Close p v)
  (match v
    [(struct Close (type name))
     (with-length-out p #\C
       (io:write-byte/char p (statement/portal->char type))
       (io:write-null-terminated-string p name))]))

(define-struct CloseComplete () #:transparent)
(define (parse:CloseComplete p)
  (with-length-in p #\3
    (make-CloseComplete)))

(define-struct CommandComplete (command) #:transparent)
(define (parse:CommandComplete p)
  (with-length-in p #\C
    (let* ([command (io:read-null-terminated-string p)])
      (make-CommandComplete (string->command-alist command)))))

(define-struct CopyInResponse (format column-formats) #:transparent)
(define (parse:CopyInResponse p)
  (with-length-in p #\G
    (let* ([format (io:read-byte p)]
           [column-formats
            (for/list ([i (in-range (io:read-int16 p))])
              (io:read-int16 p))])
      (make-CopyInResponse format column-formats))))

(define-struct CopyOutResponse (format column-formats) #:transparent)
(define (parse:CopyOutResponse p)
  (with-length-in p #\H
    (let* ([format (io:read-byte p)]
           [column-formats
            (for/list ([i (in-range (io:read-int16 p))])
              (io:read-int16 p))])
      (make-CopyOutResponse format column-formats))))

(define-struct DataRow (data) #:transparent)
(define (parse:DataRow p)
  (let ([len (io:read-int32 p)])
    (make-DataRow (read-bytes (- len 4) p))))

(define (bytes->row buf type-readers)
  (let* ([columns (integer-bytes->integer buf #t #t 0 2)]
         [row (make-vector columns)])
    (let loop ([i 0] [type-readers type-readers] [start 2])
      (when (< i columns)
        (let ([len (integer-bytes->integer buf #t #t start (+ start 4))]
              [start* (+ start 4)])
          (vector-set! row i
                       (if (= len -1)
                           sql-null
                           ((car type-readers) buf start* (+ start* len))))
          (let ([next (+ start* (max 0 len))])
            (loop (add1 i) (cdr type-readers) next)))))
    row))

(define-struct Describe (type name) #:transparent)
(define (write:Describe p v)
  (match v
    [(struct Describe (type name))
     (with-length-out p #\D
       (io:write-byte/char p (statement/portal->char type))
       (io:write-null-terminated-string p name))]))

(define-struct EmptyQueryResponse () #:transparent)
(define (parse:EmptyQueryResponse p)
  (with-length-in p #\I
    (make-EmptyQueryResponse)))

(define-struct Execute (portal row-limit) #:transparent)
(define (write:Execute p v)
  (match v
    [(struct Execute (portal row-limit))
     (with-length-out p #\E
       (io:write-null-terminated-string p portal)
       (io:write-int32 p row-limit))]))

(define-struct Flush () #:transparent)
(define (write:Flush p v)
  (with-length-out p #\H))

;; Added in PostgreSQL 9.3
(define-struct NegotiateProtocolVersion (major minor unrecognized-options) #:transparent)
(define (parse:NegotiateProtocolVersion p)
  (with-length-in p #\v
    (define last-version (io:read-int32 p))
    (define n-options (io:read-int32 p))
    (define unrecognized-options
      (for/list ([i (in-range n-options)])
        (io:read-null-terminated-string p)))
    (NegotiateProtocolVersion (bitwise-bit-field last-version 16 32)
                              (bitwise-bit-field last-version 0 16)
                              unrecognized-options)))

(define-struct NoData () #:transparent)
(define (parse:NoData p)
  (with-length-in p #\n
    (make-NoData)))

(define-struct NotificationResponse (process-id channel payload) #:transparent)
(define (parse:NotificationResponse p)
  (with-length-in p #\A
    (let* ([process-id (io:read-int32 p)]
           [channel (io:read-null-terminated-string p)]
           [payload (io:read-null-terminated-string p)])
      (make-NotificationResponse process-id channel payload))))

(define-struct ParameterDescription (type-oids) #:transparent)
(define (parse:ParameterDescription p)
  (with-length-in p #\t
    (define param-count (io:read-uint16 p))
    (let* ([type-oids
            (for/list ([i (in-range param-count)])
              (io:read-int32 p))])
      ;; If the statement has 2^16 or more parameters, then PostgreSQL sends an inaccurate
      ;; param-count (masked to 16 LSBs), but it sends *all* of the parameter types.
      ;; So detect when message has content left over after param-count types.
      (cond [(eof-object? (peek-byte p))
             (make-ParameterDescription type-oids)]
            [else 'too-many-parameters]))))

(define-struct ParameterStatus (name value) #:transparent)
(define (parse:ParameterStatus p)
  (with-length-in p #\S
    (let* ([name (io:read-null-terminated-string p)]
           [value (io:read-null-terminated-string p)])
      (make-ParameterStatus name value))))

(define-struct Parse (name query type-oids) #:transparent)
(define (write:Parse p v)
  (match v
    [(struct Parse (name query type-oids))
     (with-length-out p #\P
       (io:write-null-terminated-string p name)
       (io:write-null-terminated-string p query)
       (io:write-int16 p (length type-oids))
       (for ([type-oid (in-list type-oids)])
         (io:write-int32 p type-oid)))]))

(define-struct ParseComplete () #:transparent)
(define (parse:ParseComplete p)
  (with-length-in p #\1
    (make-ParseComplete)))

(define-struct PasswordMessage (password) #:transparent)
(define (write:PasswordMessage p v)
  (match v
    [(struct PasswordMessage (password))
     (with-length-out p #\p
       (io:write-null-terminated-string p password))]))

(define-struct PortalSuspended () #:transparent)
(define (parse:PortalSuspended p)
  (with-length-in p #\s
    (make-PortalSuspended)))

(define-struct Query (query) #:transparent)
(define (write:Query p v)
  (match v
    [(struct Query (query))
     (with-length-out p #\Q
       (io:write-null-terminated-string p query))]))

(define-struct ReadyForQuery (transaction-status) #:transparent)
(define (parse:ReadyForQuery p)
  (with-length-in p #\Z
    (let* ([transaction-status
            (char->transaction-status (io:read-byte/char p))])
      (make-ReadyForQuery transaction-status))))

(define-struct RowDescription (fields) #:transparent)
(define (parse:RowDescription p)
  (with-length-in p #\T
    (define field-count (io:read-uint16 p))
    (let* ([fields
            (for/list ([i (in-range field-count)])
              (let* ([name (io:read-null-terminated-string p)]
                     [table-oid (io:read-int32 p)]
                     [column-attid (io:read-int16 p)]
                     [type-oid (io:read-int32 p)]
                     [type-size (io:read-int16 p)]
                     [type-mod (io:read-int32 p)]
                     [format-code (io:read-int16 p)])
                (vector name table-oid column-attid type-oid type-size type-mod format-code)))])
      ;; See comment for ParameterDescription
      (cond [(eof-object? (peek-byte p))
             (make-RowDescription fields)]
            [else 'too-many-fields]))))

(define-struct SASLInitialResponse (method content) #:transparent)
(define (write:SASLInitialResponse p v)
  (match v
    [(SASLInitialResponse method content)
     (with-length-out p #\p
       (io:write-null-terminated-string p method)
       (cond [(bytes? content)
              (io:write-int32 p (bytes-length content))
              (io:write-bytes p content)]
             [(string? content)
              (io:write-int32 p (string-utf-8-length content))
              (write-string content p)]
             [else (io:write-int32 p -1)]))]))

(define-struct SASLResponse (content) #:transparent)
(define (write:SASLResponse p v)
  (match v
    [(SASLResponse content)
     (with-length-out p #\p
       (cond [(bytes? content) (write-bytes content p)]
             [(string? content) (write-string content p)]))]))

(define-struct Sync () #:transparent)
(define (write:Sync p v)
  (with-length-out p #\S))

(define-struct Terminate () #:transparent)
(define (write:Terminate p v)
  (with-length-out p #\X))

;; ========================================

(define (write-message msg port)
  (cond
    [(Sync? msg) (write:Sync port msg)]
    [(Parse? msg) (write:Parse port msg)]
    [(Describe? msg) (write:Describe port msg)]
    [(Bind? msg) (write:Bind port msg)]
    [(Execute? msg) (write:Execute port msg)]
    [(Flush? msg) (write:Flush port msg)]
    [(Query? msg) (write:Query port msg)]
    [(Close? msg) (write:Close port msg)]
    [(Terminate? msg) (write:Terminate port msg)]
    [(StartupMessage? msg) (write:StartupMessage port msg)]
    [(PasswordMessage? msg) (write:PasswordMessage port msg)]
    [(SSLRequest? msg) (write:SSLRequest port msg)]
    [(CancelRequest? msg) (write:CancelRequest port msg)]
    [(SASLInitialResponse? msg) (write:SASLInitialResponse port msg)]
    [(SASLResponse? msg) (write:SASLResponse port msg)]
    [else (error/internal* 'write-message "unknown message type"
                           '("message" value) msg)]))

(define (parse-server-message p)
  (let ([c (read-char p)])
    (case c
      ((#\R) (parse:Authentication p))
      ((#\E) (parse:ErrorResponse p))
      ((#\N) (parse:NoticeResponse p))
      ((#\K) (parse:BackendKeyData p))
      ((#\2) (parse:BindComplete p))
      ((#\3) (parse:CloseComplete p))
      ((#\C) (parse:CommandComplete p))
      ((#\G) (parse:CopyInResponse p))
      ((#\H) (parse:CopyOutResponse p))
      ((#\D) (parse:DataRow p))
      ((#\I) (parse:EmptyQueryResponse p))
      ((#\v) (parse:NegotiateProtocolVersion p))
      ((#\n) (parse:NoData p))
      ((#\A) (parse:NotificationResponse p))
      ((#\t) (parse:ParameterDescription p))
      ((#\S) (parse:ParameterStatus p))
      ((#\1) (parse:ParseComplete p))
      ((#\s) (parse:PortalSuspended p))
      ((#\Z) (parse:ReadyForQuery p))
      ((#\T) (parse:RowDescription p))
      (else
       (if (eof-object? c)
           c
           (error/internal* 'parse-server-message "unknown message header"
                            '("header" value) c))))))

;; ========================================
;; Helpers

(define (string/f->string/sql-null b)
  (if b b sql-null))

(define (string/sql-null->string/f b)
  (if (sql-null? b) #f b))

(define (char->message-tag c)
  (case c
    [(#\S) 'severity]
    [(#\V) 'severity*]
    [(#\C) 'code]
    [(#\M) 'message]
    [(#\D) 'detail]
    [(#\H) 'hint]
    [(#\P) 'position]
    [(#\p) 'internal-position]
    [(#\q) 'internal-query]
    [(#\W) 'where]
    [(#\s) 'schema]
    [(#\t) 'table]
    [(#\c) 'column]
    [(#\d) 'datatype]
    [(#\n) 'constraint]
    [(#\F) 'file]
    [(#\L) 'line]
    [(#\R) 'routine]))

(define (char->statement/portal c)
  (case c
    [(#\S) 'statement]
    [(#\P) 'portal]))
(define (statement/portal->char sp)
  (case sp
    [(statement) #\S]
    [(portal) #\P]))

(define (char->transaction-status c)
  (case c
    [(#\I) 'idle]
    [(#\T) 'transaction]
    [(#\E) 'failed]))
(define (transaction-status->char ts)
  (case ts
    [(idle) #\I]
    [(transaction) #\T]
    [(failed) #\E]))

(define (string->command-alist s)
  (cond [(regexp-match #rx"^INSERT ([0-9]*) ([0-9]*) *$" s)
         => (lambda (m)
              `((insert-id . ,(let ([oid (string->number (cadr m))])
                                (if (zero? oid) #f oid)))
                (affected-rows . ,(string->number (caddr m)))))]
        [(regexp-match #rx"^DELETE ([0-9]*) *$" s)
         => (lambda (m)
              `((affected-rows . ,(string->number (cadr m)))))]
        [(regexp-match #rx"^UPDATE ([0-9]*) *$" s)
         => (lambda (m)
              `((affected-rows . ,(string->number (cadr m)))))]
        #|
        [(regexp-match #rx"^SELECT *$" s) ...]
        [(regexp-match #rx"^MOVE ([0-9]*) *$" s) ...]
        [(regexp-match #rx"^FETCH ([0-9]*) *$" s) ...]
        [(regexp-match #rx"^(CREATE|ALTER|DROP) ([A-Z]*) *$" s) ...]
        |#
        [else '()]))


;; dvec layout is #(name table-oid col-oid typeid typelen typemod text/binary)

(define (field-dvec->typeid dvec)
  (vector-ref dvec 3))

(define (field-dvec->field-info dvec)
  (match dvec
    [(vector name table column type-oid type-size type-mod format-code)
     `((name . ,name)
       (typeid . ,type-oid)
       (type-size . ,type-size)
       (type-mod . ,type-mod))]))
