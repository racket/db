#lang racket/base
(require racket/class
         racket/list
         racket/math
         racket/match
         ffi/unsafe
         ffi/unsafe/atomic
         ffi/unsafe/custodian
         db/private/generic/interfaces
         db/private/generic/common
         db/private/generic/ffi-common
         db/private/generic/prepared
         db/private/generic/sql-data
         db/private/generic/sql-convert
         "ffi.rkt"
         "ffi-constants.rkt"
         "../generic/sql-convert.rkt"
         "dbsystem.rkt")
(provide connection%
         handle-status*
         dbsystem)

;; ----------------------------------------
(module yn racket/base
  (require racket/match)
  (provide (all-defined-out))

  ;; A (YN V W) is either
  ;; - (yes V)
  ;; - (no W)
  ;; If omitted, W defaults to (-> Error).
  (struct yes (v) #:prefab)
  (struct no (v) #:prefab)
  (define (yn? v) (or (yes? v) (no? v)))

  ;; yn-ref : (YN V) -> V
  (define (yn-ref c)
    (match c [(yes v) v] [(no err) (err)]))

  ;; yn-ref-values : (YN (list X ...)) -> (values X ...)
  (define (yn-ref-values c)
    (match c [(yes vs) (apply values vs)] [(no err) (err)]))

  (define-syntax yndo
    (syntax-rules (=>)
      [(yndo)
       (yes (void))]
      [(yndo => e ...)
       (let () e ...)]
      [(yndo [#:do e ...] . more)
       (let () e ... (yndo . more))]
      [(yndo [okpat rhs] . more)
       (yn-bind rhs (match-lambda [okpat (yndo . more)]))]))

  (define (yn-bind c f [no-f #f])
    (match c
      [(yes v) (f v)]
      [(no w) (if no-f (no (no-f w)) c)]))

  (define (yn-map c f [no-f #f])
    (match c
      [(yes v) (yes (f v))]
      [(no w) (if no-f (no (no-f w)) c)]))

  ;; (for/yn (clause ...) body) : (YN (Listof V W)), where body : (YN V W)
  (define-syntax for/yn
    (syntax-rules ()
      [(for/yn (clause ...) . body)
       (for/fold ([acc (yes null)] #:result (yn-map acc reverse))
                 (clause ...)
         (yndo [vs acc]
               [v (let () . body)]
               => (yes (cons v vs))))]))

  (define (yn-refs/void cs)
    (for-each yn-ref cs)))
(require (submod "." yn))
;; ----------------------------------------

;; ODBC connections do not use statement-cache%
;;  - safety depends on sql dialect
;;  - transaction interactions more complicated

(define connection%
  (class* (ffi-connection-mixin transactions%) (connection<%>)
    (inherit call-with-lock
             call-with-lock*
             add-delayed-call!
             get-tx-status
             set-tx-status!
             check-valid-tx-status
             check-statement/tx
             use-worker-mode
             worker-call)
    (init connect-info  ;; (list Symbol Symbol (_sqlhdbc -> (YN Void)))
          quirks        ;; (Listof Symbol)
          worker-mode
          strict-parameter-types?)
    (init-private char-mode   ;; (U 'wchar 'utf-8 'latin-1)
                  notice-handler)
    (super-new)

    (define -db #f)         ;; _sqlhdbc
    (define -env #f)        ;; _sqlhenv
    (define -unregister #f) ;; (-> Void), unregister finalizer/custodian
    (define -db-connected? #f)  ;; Boolean

    ;; Must finalize all stmts before closing db, but also want stmts to be
    ;; independently finalizable. So db needs strong refs to stmts (but no
    ;; strong refs to prepared-statement% wrappers).
    (define statement-table (make-hasheq)) ;; hasheq[_sqlhstmt => #t]

    ;; Custodian shutdown can cause disconnect even in middle of operation (with
    ;; lock held), so -db field may become #f at any time outside of atomic region
    ;; (uninterruptible mode is insufficient!). So use (A _) around any FFI calls,
    ;; check still connected.
    (define-syntax-rule (A* #:db db e ...)
      (begin (start-atomic) (begin0 (let ([db -db]) e ...) (end-atomic))))
    (define-syntax-rule (A #:db db e ...)
      (A* #:db db (unless db (end-atomic) (error/disconnect-in-lock 'odbc)) e ...))

    ;; If worker thread used (see ffi-common), then disconnect sets -db field in
    ;; atomic mode, then finishes disconnect in worker thread. So uninterruptible
    ;; mode sufficient in worker (-db field may change, but db pointer still valid).
    (define-syntax-rule (FA* worker? #:db db e ...)
      (begin (if worker? (start-uninterruptible) (start-breakable-atomic))
             (begin0 (let ([db -db])
                       (unless db
                         (if worker? (end-uninterruptible) (end-breakable-atomic))
                         (error/disconnect-in-lock 'odbc))
                       e ...)
               (if worker? (end-uninterruptible) (end-breakable-atomic)))))

    ;; Handle expr which produces SQLRETURN as first value, return yn wrapper.
    (define-syntax-rule (YN who expr diag-handle opt ...)
      (handle/yn who (call-with-values (lambda () expr) list) diag-handle opt ...))

    (let ()
      (match-define (list who opname connect) connect-info)
      (use-worker-mode worker-mode)
      (define delayed-notices null) ;; mutated, (Listof (-> Void))
      (define saved-notice-handler notice-handler)
      (set! notice-handler
            (lambda (sqlstate message)
              (set! delayed-notices
                    (cons (lambda () (saved-notice-handler sqlstate message))
                          delayed-notices))))
      (begin
        (start-atomic)
        (register-finalizer-and-custodian-shutdown
         this shutdown-connection
         #:custodian-available
         (lambda (unregister)
           (set! -unregister unregister))
         #:custodian-unavailable
         (lambda (register-finalizer)
           (begin (end-atomic) (error who "custodian shut down"))))
        (define (cleanup env dbc)
          (-unregister this)
          (when env (SQLFreeHandle SQL_HANDLE_ENV env))
          (when dbc (SQLFreeHandle SQL_HANDLE_DBC dbc)))
        (define-values (env-status env) (SQLAllocHandle SQL_HANDLE_ENV #f))
        (match (YN who env-status #f 'SQLAllocHandle)
          [(yes _) (void)]
          [(no err) (begin (cleanup env #f) (end-atomic) (err))])
        (match (YN who (SQLSetEnvAttr env SQL_ATTR_ODBC_VERSION SQL_OV_ODBC3) env 'SQLSetEnvAttr)
          [(yes _) (void)]
          [(no err) (begin (cleanup env #f) (end-atomic) (err))])
        (define-values (db-status db) (SQLAllocHandle SQL_HANDLE_DBC env))
        (match (YN who db-status env 'SQLAllocHandle)
          [(yes _) (void)]
          [(no err) (begin (cleanup env db) (end-atomic) (err))])
        (set! -db db)
        (set! -env env)
        (end-atomic))
      (for ([notice (in-list (reverse delayed-notices))]) (notice))
      (set! notice-handler saved-notice-handler)
      (yn-ref
       (worker-call
        (lambda (worker?)
          (FA* worker? #:db db
               (yndo [_ (YN who (connect db) db opname)]
                     [#:do (set! -db-connected? #t)]))))))

    (define quirk-c-bigint-ok?  (not (memq 'no-c-bigint quirks)))
    (define quirk-c-numeric-ok? (not (memq 'no-c-numeric quirks)))
    (define quirk-fix-sqllen?   (not (memq 'no-fix-sqllen quirks)))

    (define use-describe-param?
      (and strict-parameter-types?
           (let-values ([(status supported?)
                         (A #:db db (SQLGetFunctions db SQL_API_SQLDESCRIBEPARAM))])
             (and (ok-status? status) supported?))))

    (define/private (get-odbc-info-string who key)
      (define-values (status result) (A #:db db (SQLGetInfo-string db key)))
      (and result (string->immutable-string result)))

    (define dbms (get-odbc-info-string 'odbc-connect SQL_DBMS_NAME))
    (define odbc-info #f)

    (define/public (get-odbc-info)
      (or odbc-info
          (let ()
            (define (get key) (get-odbc-info-string 'get-odbc-info key))
            (set! odbc-info
                  (hasheq 'dbms-name (get SQL_DBMS_NAME)
                          'dbms-version (get SQL_DBMS_VER)
                          'driver-name (get SQL_DRIVER_NAME)
                          'driver-version (get SQL_DRIVER_VER)
                          'driver-odbc-version (get SQL_DRIVER_ODBC_VER)
                          'odbc-version (get SQL_ODBC_VER)))
            odbc-info)))

    ;; ----------------------------------------

    (define/public (get-dbsystem) dbsystem)
    (define/override (connected?) (and -db #t))

    (define/public (query fsym stmt cursor?)
      (call-with-lock fsym
        (lambda ()
          (check-valid-tx-status fsym)
          (query1 fsym stmt #t cursor?))))

    (define/private (query1 fsym stmt check-tx? cursor?)
      (let* ([stmt (check-statement fsym stmt cursor?)]
             [pst (statement-binding-pst stmt)]
             [params (statement-binding-params stmt)])
        (when check-tx? (check-statement/tx fsym (send pst get-stmt-type)))
        (let ([result-dvecs (send pst get-result-dvecs)])
          (for ([dvec (in-list result-dvecs)])
            (let ([typeid (field-dvec->typeid dvec)])
              (unless (supported-typeid? typeid)
                (error/unsupported-type fsym typeid)))))
        (worker-call
         (lambda (worker?)
           (query1:inner fsym worker? pst params cursor?)))))

    (define/private (check-statement fsym stmt cursor?)
      (cond [(statement-binding? stmt)
             (let ([pst (statement-binding-pst stmt)])
               (send pst check-owner fsym this stmt)
               (cond [cursor?
                      (let ([pst* (prepare1 fsym (send pst get-stmt) #f)])
                        (statement-binding pst* (statement-binding-params stmt)))]
                     [else stmt]))]
            [(string? stmt)
             (let* ([pst (prepare1 fsym stmt (not cursor?))])
               (send pst bind fsym null))]))

    ;; query1:inner : ... -> QueryResult
    (define/private (query1:inner who worker? pst params cursor?)
      (define-syntax-rule (FA #:db db e ...) (FA* worker? #:db db e ...))
      (define stmt (send pst get-handle))
      (define result-dvecs (send pst get-result-dvecs))
      (define param-bufs
        ;; Need to keep references to all bufs until after SQLExecute.
        (for/list ([i (in-naturals 1)]
                   [param (in-list params)]
                   [param-typeid (in-list (send pst get-param-typeids))])
          (load-param who worker? stmt i param param-typeid)))
      (yn-ref (FA #:db db (YN who (SQLExecute stmt) stmt 'SQLExecute)))
      (void/reference-sink param-bufs)
      (FA #:db db (-set-result-descriptors stmt result-dvecs))
      (define typeids (map field-dvec->typeid result-dvecs))
      (cond [(and (pair? result-dvecs) (not cursor?))
             (define rows (get-rows who worker? stmt typeids #f +inf.0))
             (send pst after-exec #f)
             (rows-result (map field-dvec->field-info result-dvecs) rows)]
            [(and (pair? result-dvecs) cursor?)
             (define field-infos (map field-dvec->field-info result-dvecs))
             (cursor-result field-infos pst (list typeids (box #f)))]
            [else
             (send pst after-exec #f)
             (simple-result '())]))

    (define/public (fetch/cursor who cursor fetch-size)
      (let ([pst (cursor-result-pst cursor)]
            [extra (cursor-result-extra cursor)])
        (send pst check-owner who this pst)
        (call-with-lock who
          (lambda ()
            (match-define (list typeids end-box) extra)
            (cond [(unbox end-box) #f]
                  [else
                   (worker-call
                    (lambda (worker?)
                      (define stmt (send pst get-handle))
                      (define rows (get-rows who worker? stmt typeids end-box fetch-size))
                      (when (unbox end-box) (send pst after-exec #f))
                      rows))])))))

    ;; load-param : ... -> Any
    ;; Returns refs that must not be GC'd until after SQLExecute.
    (define/private (load-param who worker? stmt i param typeid)
      (define-syntax-rule (FA #:db db e ...) (FA* worker? #:db db e ...))
      ;; typeid-or : Integer -> Integer
      ;; Replace SQL_UNKNOWN_TYPE with given alternative typeid
      (define (typeid-or alt-typeid) (if (= typeid SQL_UNKNOWN_TYPE) alt-typeid typeid))

      ;; bind : Integer Integer (U Bytes #f) [Byte Byte] -> Any
      ;; NOTE: param buffers must not move between bind and execute.
      ;; Returns refs that must not be GC'd until after SQLExecute.
      (define (bind ctype sqltype value [prec 0] [scale 0])
        (define lenbuf
          (bytes->non-moving-pointer
           (integer->integer-bytes (if value (bytes-length value) SQL_NULL_DATA) sizeof-SQLLEN #t)))
        (define-values (valbuf vallen)
          (cond [(bytes? value) (values (bytes->non-moving-pointer value) (bytes-length value))]
                [(eq? value #f) (values #f 0)]
                [else (error 'bind "internal error: bad value: ~e" value)]))
        (yn-ref
         (FA #:db db
             (YN who (SQLBindParameter stmt i ctype sqltype prec scale valbuf vallen lenbuf) stmt)))
        (if valbuf (cons valbuf lenbuf) lenbuf))

      ;; do-load-number : (U Real (Cons Integer Nat)) Integer -> Any
      (define (do-load-number param typeid)
        (cond [(or (= typeid SQL_NUMERIC) (= typeid SQL_DECIMAL))
               ;; param = (cons mantissa exponent), scaled integer
               (define ma (car param))
               (define ex (cdr param))
               (define prec (if (zero? ma) 1 (+ 1 (order-of-magnitude (abs ma)))))
               (define prec* (max prec ex))
               (cond [quirk-c-numeric-ok?
                      ;; ODBC docs claim max precision is 15 ...
                      (define sign-byte (if (negative? ma) 0 1))
                      (define digits-bytess
                        ;; 16 bytes of unsigned little-endian data (4 chunks of 4 bytes)
                        (let loop ([i 0] [ma (abs ma)])
                          (if (< i 4)
                              (let-values ([(q r) (quotient/remainder ma (expt 2 32))])
                                (cons (integer->integer-bytes r 4 #f #f)
                                      (loop (add1 i) q)))
                              null)))
                      (define numeric-bytes
                        (apply bytes-append (bytes prec* ex sign-byte) digits-bytess))
                      ;; Call bind first, then set descriptor attributes.
                      (begin0 (bind SQL_C_NUMERIC typeid numeric-bytes prec* ex)
                        (FA #:db db
                            (let ([hdesc (SQLGetStmtAttr/HDesc stmt SQL_ATTR_APP_PARAM_DESC)])
                              (-set-numeric-descriptors hdesc i prec* ex numeric-bytes))))]
                     [else
                      (define s (scaled-integer->decimal-string ma ex))
                      (bind SQL_C_CHAR typeid (string->bytes/latin-1 s) prec* ex)])]
              [(or (= typeid SQL_INTEGER)
                   (= typeid SQL_SMALLINT)
                   (= typeid SQL_TINYINT))
               (bind SQL_C_LONG typeid (integer->integer-bytes param sizeof-SQLLONG #t))]
              [(or (= typeid SQL_BIGINT))
               ;; Oracle errors without diagnostic record (!!) on BIGINT param
               ;; -> http://stackoverflow.com/questions/338609
               (cond [quirk-c-bigint-ok?
                      (bind SQL_C_SBIGINT typeid (integer->integer-bytes param 8 #t))]
                     [else
                      (bind SQL_C_CHAR typeid (string->bytes/latin-1 (number->string param)))])]
              [(or (= typeid SQL_FLOAT)
                   (= typeid SQL_REAL)
                   (= typeid SQL_DOUBLE))
               (bind SQL_C_DOUBLE typeid (real->floating-point-bytes (exact->inexact param) 8))]
              ;; -- UNKNOWN --
              [(= typeid SQL_UNKNOWN_TYPE)
               (cond [(int32? param)
                      (do-load-number param SQL_INTEGER)]
                     [(int64? param)
                      (do-load-number param SQL_BIGINT)]
                     [else ;; real
                      (do-load-number param SQL_DOUBLE)])]
              ;; -- Otherwise error --
              [else (raise
                     (lambda ()
                       (error who "internal error: bad typeid\n  typeid: ~a\n  parameter: ~e"
                              typeid param)))]))

      ;; -- load-param body --
      (cond [(or (real? param) (pair? param))
             (do-load-number param typeid)]
            [(string? param)
             (case char-mode
               ((wchar)
                (bind SQL_C_WCHAR (typeid-or SQL_WVARCHAR) (string->wbuf param)))
               ((utf-8)
                (bind SQL_C_CHAR (typeid-or SQL_VARCHAR) (string->bytes/utf-8 param)))
               ((latin-1)
                (bind SQL_C_CHAR (typeid-or SQL_VARCHAR)
                      (string->bytes/latin-1 param (char->integer #\?)))))]
            [(bytes? param)
             (bind SQL_C_BINARY (typeid-or SQL_BINARY) param (bytes-length param))]
            [(boolean? param)
             (bind SQL_C_LONG SQL_BIT
                   (integer->integer-bytes (if param 1 0) sizeof-SQLLONG #t))]
            [(sql-date? param)
             (bind SQL_C_TYPE_DATE SQL_TYPE_DATE
                   (let* ([x param]
                          [y (sql-date-year x)]
                          [m (sql-date-month x)]
                          [d (sql-date-day x)])
                     (bytes-append (integer->integer-bytes y 2 #t)
                                   (integer->integer-bytes m 2 #f)
                                   (integer->integer-bytes d 2 #f))))]
            [(sql-time? param)
             (cond [(= typeid SQL_SS_TIME2)
                    (bind SQL_C_BINARY typeid
                          (let* ([x param]
                                 [h (sql-time-hour x)]
                                 [m (sql-time-minute x)]
                                 [s (sql-time-second x)]
                                 [ns (sql-time-nanosecond x)])
                            (bytes-append (integer->integer-bytes h 2 #f)
                                          (integer->integer-bytes m 2 #f)
                                          (integer->integer-bytes s 2 #f)
                                          (integer->integer-bytes 0 2 #f)
                                          (let ([ns (* 100 (quotient ns 100))])
                                            (integer->integer-bytes ns 4 #f))))
                          12 7)]
                   [else
                    (bind SQL_C_TYPE_TIME SQL_TYPE_TIME
                          (let* ([x param]
                                 [h (sql-time-hour x)]
                                 [m (sql-time-minute x)]
                                 [s (sql-time-second x)])
                            (bytes-append (integer->integer-bytes h 2 #f)
                                          (integer->integer-bytes m 2 #f)
                                          (integer->integer-bytes s 2 #f))))])]
            [(sql-time? param)
             (bind SQL_C_TYPE_TIME SQL_TYPE_TIME
                   (let* ([x param]
                          [h (sql-time-hour x)]
                          [m (sql-time-minute x)]
                          [s (sql-time-second x)])
                     (bytes-append (integer->integer-bytes h 2 #f)
                                   (integer->integer-bytes m 2 #f)
                                   (integer->integer-bytes s 2 #f))))]
            [(sql-timestamp? param)
             (bind SQL_C_TYPE_TIMESTAMP (typeid-or SQL_TYPE_TIMESTAMP)
                   (let ([x param])
                     (bytes-append
                      (integer->integer-bytes (sql-timestamp-year x) 2 #f)
                      (integer->integer-bytes (sql-timestamp-month x) 2 #f)
                      (integer->integer-bytes (sql-timestamp-day x) 2 #f)
                      (integer->integer-bytes (sql-timestamp-hour x) 2 #f)
                      (integer->integer-bytes (sql-timestamp-minute x) 2 #f)
                      (integer->integer-bytes (sql-timestamp-second x) 2 #f)
                      (integer->integer-bytes (sql-timestamp-nanosecond x) 4 #f))))]
            [(sql-null? param)
             (bind SQL_C_CHAR SQL_VARCHAR #f)]
            [else
             (raise (lambda ()
                      (error who "cannot convert value to SQL type\n  value: ~e\n  typeid: ~e"
                             param typeid)))]))

    (define/private (-set-result-descriptors stmt dvecs) ;; pre: atomic
      (for ([i (in-naturals 1)]
            [dvec (in-list dvecs)])
        (define typeid (field-dvec->typeid dvec))
        (cond [(or (= typeid SQL_DECIMAL)
                   (= typeid SQL_NUMERIC))
               (define hdesc (SQLGetStmtAttr/HDesc stmt SQL_ATTR_APP_ROW_DESC))
               (define size (field-dvec->size dvec))
               (define digits (field-dvec->digits dvec))
               (-set-numeric-descriptors hdesc i size digits #f)]
              [else (void)])))

    (define/private (-set-numeric-descriptors hdesc i prec ex buf) ;; pre: atomic
      (SQLSetDescField/SmallInt hdesc i SQL_DESC_TYPE SQL_C_NUMERIC)
      (SQLSetDescField/SmallInt hdesc i SQL_DESC_PRECISION prec)
      (SQLSetDescField/SmallInt hdesc i SQL_DESC_SCALE ex)
      (when buf (SQLSetDescField/Ptr hdesc i SQL_DESC_DATA_PTR buf (bytes-length buf))))

    ;; get-rows : ... -> (Listof Vector)
    (define/private (get-rows who worker? stmt result-typeids end-box limit)
      (define-syntax-rule (FA #:db db e ...) (FA* worker? #:db db e ...))
      ;; scratchbuf: create a single buffer here to try to reduce garbage
      ;; Don't make too big; otherwise bad for queries with only small data.
      ;; Doesn't need to be large, since get-varbuf already smart for long data.
      ;; MUST be at least as large as any int/float type
      ;; SHOULD be at least as large as any structures (see uses of get-int-list)
      (define scratchbuf (make-bytes 50))
      ;; fetch* : ... -> (Listof Vector)
      (define (fetch* fetched acc)
        (cond [(< fetched limit)
               (define row (fetch))
               (cond [(vector? row)
                      (fetch* (add1 fetched) (cons row acc))]
                     [else ;; #f
                      (when end-box (set-box! end-box #t))
                      (yn-refs/void
                       (FA #:db db
                           (list (YN who (SQLFreeStmt stmt SQL_CLOSE) stmt 'SQLFreeStmt)
                                 (YN who (SQLFreeStmt stmt SQL_RESET_PARAMS) stmt 'SQLFreeStmt))))
                      (reverse acc)])]
              [else (reverse acc)]))
      ;; fetch : ... -> (U #f Vector)
      (define (fetch)
        (define s (yn-ref-values (FA #:db db (YN who (SQLFetch stmt) stmt 'SQLFetch))))
        (cond [(= s SQL_NO_DATA) #f]
              [else ;; SQL_SUCCESS, SQL_SUCCESS_WITH_INFO
               (define column-count (length result-typeids))
               (define vec (make-vector column-count))
               (for ([i (in-range column-count)]
                     [typeid (in-list result-typeids)])
                 (vector-set! vec i (get-column who worker? stmt (add1 i) typeid scratchbuf)))
               vec]))
      ;; ----
      (fetch* 0 null))

    ;; get-column : ... -> Any
    (define/private (get-column who worker? stmt i typeid scratchbuf)
      (define-syntax-rule (FA #:db db e ...) (FA* worker? #:db db e ...))
      (define (get-int size ctype)
        (define-values (status ind)
          (yn-ref-values (FA #:db db (YN who (SQLGetData stmt i ctype scratchbuf 0) stmt))))
        (cond [(= ind SQL_NULL_DATA) sql-null]
              [else (integer-bytes->integer scratchbuf #t (system-big-endian?) 0 size)]))
      (define (get-real ctype)
        (define-values (status ind)
          (yn-ref-values (FA #:db db (YN who (SQLGetData stmt i ctype scratchbuf 0) stmt))))
        (cond [(= ind SQL_NULL_DATA) sql-null]
              [else (floating-point-bytes->real scratchbuf (system-big-endian?) 0 8)]))
      (define (get-int-list sizes ctype)
        (define buflen (apply + sizes))
        (define buf (if (<= buflen (bytes-length scratchbuf)) scratchbuf (make-bytes buflen)))
        (define-values (status ind)
          (yn-ref-values (FA #:db db (YN who (SQLGetData stmt i ctype buf 0) stmt))))
        (cond [(= ind SQL_NULL_DATA) sql-null]
              [else (parse-int-list buf 0 sizes)]))
      (define (parse-int-list buf start sizes)
        (if (pair? sizes)
            (let ([size (car sizes)])
              (cons (case size
                      [(1) (bytes-ref buf start)]
                      [(2) (integer-bytes->integer buf #f (system-big-endian?) start (+ start 2))]
                      [(4) (integer-bytes->integer buf #f (system-big-endian?) start (+ start 4))]
                      [else (error who "internal error: get-int-list bad size: ~s" size)])
                    (parse-int-list buf (+ start size) (cdr sizes))))
            null))
      (define (get-varbuf ctype ntlen convert)
        ;; ntlen is null-terminator length (1 for char data, 0 for binary, ??? for wchar)

        ;; null-terminator, there are 3 modes: binary, wchar=2, wchar=4
        ;; - binary: all done in racket, no worries
        ;; - wchar=4: passed to make_sized_char_string, so must explicitly leave ntlen \0 bytes
        ;; - wchar=2: passed to utf16_to_ucs4 (which must add \0 space, see ffi.rkt)
        ;; So for simplicity, add ntlen \0 bytes to buf (but do *not* add to data len)

        ;; ODBC docs say len-or-ind is character count for char data, but wrong:
        ;; always a byte count.

        ;; It would be nice if we could call w/ empty buffer, get total length, then
        ;; call again with appropriate buffer. But can't use NULL (works on unixodbc, but
        ;; ODBC spec says illegal and Win32 ODBC rejects). Seems unsafe to use 0-length
        ;; buffer (spec is unclear, DB2 docs say len>0...???).

        ;; loop : Bytes Nat (Listof Bytes) -> Any
        ;; start is next place to write, but data starts at 0
        ;; rchunks is reversed list of previous chunks (for data longer than scratchbuf)
        ;; Small data done in one iteration; most long data done in two. Only long data
        ;; without known size (???) should take more than two iterations.
        (define (loop buf start rchunks)
          (define-values (status len-or-ind0)
            (yn-ref-values
             (FA #:db db (YN who (SQLGetData stmt i ctype buf start) stmt 'SQLGetData void))))
          (define len-or-ind (if quirk-fix-sqllen? (fix-sqllen len-or-ind0) len-or-ind0))
          (cond [(= len-or-ind SQL_NULL_DATA) sql-null]
                [(= len-or-ind SQL_NO_TOTAL)
                 ;; Didn't fit in buf, and we have no idea how much more there is.
                 (let* ([data-end (- (bytes-length buf) ntlen)])
                   (loop buf 0 (cons (subbytes buf 0 data-end) rchunks)))]
                [else
                 (let ([len (+ start len-or-ind)])
                   (cond [(<= 0 len (- (bytes-length buf) ntlen))
                          ;; fit in buf
                          (cond [(pair? rchunks)
                                 ;; add ntlen bytes for null-terminator...
                                 (let* ([chunk (subbytes buf 0 (+ len ntlen))]
                                        [chunks (append (reverse rchunks) (list chunk))]
                                        [complete (apply bytes-append chunks)])
                                   ;; ... but compensate so len is correct
                                   (convert complete
                                            (- (bytes-length complete) ntlen)
                                            #t))]
                                [else
                                 ;; buf already null-terminated, len correct
                                 (convert buf len #f)])]
                         [else
                          ;; didn't fit in buf, but we know how much more there is
                          (let* ([len-got (- (bytes-length buf) ntlen)]
                                 [newbuf (make-bytes (+ len ntlen))])
                            (bytes-copy! newbuf 0 buf start len-got)
                            (loop newbuf len-got rchunks))]))]))
        (loop scratchbuf 0 null))

      (define (get-string/latin-1)
        (get-varbuf SQL_C_CHAR 1
                    (lambda (buf len _fresh?)
                      (bytes->string/latin-1 buf #f 0 len))))
      (define (get-string/utf-8)
        (get-varbuf SQL_C_CHAR 1
                    (lambda (buf len _fresh?)
                      (bytes->string/utf-8 buf #f 0 len))))
      (define (get-string)
        (case char-mode
          ((wchar)
           (get-varbuf SQL_C_WCHAR WCHAR-SIZE (case WCHAR-SIZE ((2) mkstr2) ((4) mkstr4))))
          ((utf-8)
           (get-string/utf-8))
          ((latin-1)
           (get-string/latin-1))))
      (define (get-bytes)
        (get-varbuf SQL_C_BINARY 0
                    (lambda (buf len fresh?)
                      ;; avoid copying long data twice:
                      (if (and fresh? (= len (bytes-length buf)))
                          buf
                          (subbytes buf 0 len)))))
      (define (get-col-fail what v)
        (raise (lambda () (error who "internal error: cannot get ~s from ~e" what v))))

      (cond [(or (= typeid SQL_CHAR)
                 (= typeid SQL_VARCHAR)
                 (= typeid SQL_LONGVARCHAR)
                 (= typeid SQL_WCHAR)
                 (= typeid SQL_WVARCHAR)
                 (= typeid SQL_WLONGVARCHAR))
             (get-string)]
            [(or (= typeid SQL_DECIMAL)
                 (= typeid SQL_NUMERIC))
             (cond [quirk-c-numeric-ok?
                    (match (get-int-list '(1 1 1 4 4 4 4) SQL_ARD_TYPE)
                      [(list precision scale sign-flag ma0 ma1 ma2 ma3)
                       (define sign (case sign-flag [(0) -1] [(1) 1]))
                       (define ma
                         (+ (arithmetic-shift ma0 (* 32 0))
                            (arithmetic-shift ma1 (* 32 1))
                            (arithmetic-shift ma2 (* 32 2))
                            (arithmetic-shift ma3 (* 32 3))))
                       (* sign ma (expt 10 (- scale)))]
                      [(? sql-null?) sql-null])]
                   [else
                    (define s (get-string/latin-1))
                    (cond [(string->number s 10 'number-or-false 'decimal-as-exact) => values]
                          [else (get-col-fail "NUMERIC" s)])])]
            [(or (= typeid SQL_SMALLINT)
                 (= typeid SQL_INTEGER)
                 (= typeid SQL_TINYINT))
             (get-int 4 SQL_C_LONG)]
            [(or (= typeid SQL_BIGINT))
             (cond [quirk-c-bigint-ok?
                    (get-int 8 SQL_C_SBIGINT)]
                   [else
                    (define s (get-string/latin-1))
                    (cond [(string->number s 10 'number-or-false 'decimal-as-exact) => values]
                          [else (get-col-fail "BIGINT" s)])])]
            [(or (= typeid SQL_REAL)
                 (= typeid SQL_FLOAT)
                 (= typeid SQL_DOUBLE))
             (get-real SQL_C_DOUBLE)]
            [(or (= typeid SQL_BIT))
             (define n (get-int 4 SQL_C_LONG))
             (case n [(0) #f] [(1) #t] [else (get-col-fail "SQL_BIT" n)])]
            [(or (= typeid SQL_BINARY)
                 (= typeid SQL_VARBINARY))
             (get-bytes)]
            [(= typeid SQL_TYPE_DATE)
             (match (get-int-list '(2 2 2) SQL_C_TYPE_DATE)
               [(list y m d) (sql-date y m d)]
               [(? sql-null?) sql-null])]
            [(= typeid SQL_TYPE_TIME)
             (match (get-int-list '(2 2 2) SQL_C_TYPE_TIME)
               [(list h m s) (sql-time h m s 0 #f)]
               [(? sql-null?) sql-null])]
            [(= typeid SQL_SS_TIME2)
             (match (get-int-list '(2 2 2 2 4) SQL_C_BINARY)
               [(list h m s _pad ns) (sql-time h m s ns #f)]
               [(? sql-null?) sql-null])]
            [(= typeid SQL_TYPE_TIMESTAMP)
             (match (get-int-list '(2 2 2  2 2 2 4) SQL_C_TYPE_TIMESTAMP)
               [(list yy mm dd  h m s ns) (sql-timestamp yy mm dd h m s ns #f)]
               [(? sql-null?) sql-null])]
            [else (get-string)]))

    (define/public (prepare fsym stmt close-on-exec?)
      (call-with-lock fsym
        (lambda ()
          (check-valid-tx-status fsym)
          (prepare1 fsym stmt close-on-exec?))))

    (define/private (prepare1 who sql close-on-exec?)
      (define-values (stmt param-typeids result-dvecs)
        (yn-ref-values
         (worker-call
          (lambda (worker?)
            (define-syntax-rule (FA #:db db e ...) (FA* worker? #:db db e ...))
            (FA #:db db
                (let-values ([(status stmt) (SQLAllocHandle SQL_HANDLE_STMT db)])
                  (match (yndo [_ (YN who status db 'SQLAllocHandle)]
                               [_ (YN who (SQLPrepare stmt sql) stmt 'SQLPrepare)]
                               [param-typeids (-describe-params who stmt)]
                               [result-dvecs (-describe-columns who stmt)]
                               => (yes (list stmt param-typeids result-dvecs)))
                    [(yes vs)
                     (hash-set! statement-table stmt #t)
                     (yes vs)]
                    [(no err)
                     (when stmt (SQLFreeHandle SQL_HANDLE_STMT stmt))
                     (no err)])))))))
      (new prepared-statement%
           (handle stmt)
           (close-on-exec? close-on-exec?)
           (param-typeids param-typeids)
           (result-dvecs result-dvecs)
           (stmt sql)
           (stmt-type (classify-odbc-sql sql))
           (owner this)))

    ;; -describe-params : Symbol Stmt -> (YN (Listof TypeID))
    (define/private (-describe-params who stmt)
      (yndo [(list _ param-count) (YN who (SQLNumParams stmt) stmt)]
            => (for/yn ([i (in-range 1 (add1 param-count))])
                 (cond [use-describe-param?
                        (yndo [(list _ type _ _ _)
                               (YN who (SQLDescribeParam stmt i) stmt)]
                              => (yes type))]
                       [else (yes SQL_UNKNOWN_TYPE)]))))

    ;; -describe-columns : Symbol Stmt -> (YN (Listof DVec))
    (define/private (-describe-columns who stmt)
      (define scratchbuf (make-bytes 200))
      (yndo [(list _ result-count) (YN who (SQLNumResultCols stmt) stmt)]
            => (for/yn ([i (in-range 1 (add1 result-count))])
                 (yndo [(list _ name type size digits _)
                        (YN who (SQLDescribeCol stmt i scratchbuf) stmt)]
                       => (yes (vector name type size digits))))))

    (define/override (-stage-disconnect)
      ;; Stage 1 (atomic):
      ;; Save and clear fields
      (define dont-gc this%)
      (define db -db)
      (define env -env)
      (define db-connected? -db-connected?)
      (define stmts (hash-keys statement-table))
      (set! -db #f)
      (set! -env #f)
      (set! -db-connected? #f)
      (hash-clear! statement-table)
      ;; Unregister custodian shutdown, unless called from custodian.
      (when -unregister
        (-unregister this)
        (set! -unregister #f))
      (lambda ()
        ;; Stage 2 (worker or atomic):
        ;; Free all of connection's prepared statements. This will leave
        ;; pst objects with dangling foreign objects, so don't try to free
        ;; them again---check that -db is not-#f.
        (define yns
          (list
           (for/list ([stmt (in-list stmts)])
             (list (YN 'disconnect (SQLFreeStmt stmt SQL_CLOSE) stmt 'SQLFreeStmt)
                   (YN 'disconnect (SQLFreeHandle SQL_HANDLE_STMT stmt) stmt 'SQLFreeHandle)))
           (if db-connected?
               (YN 'disconnect (SQLDisconnect db) db 'SQLDisconnect)
               null)
           ;; If SQLFreeHandle returns error, handle still valid, use for diagnostics.
           (YN 'disconnect (SQLFreeHandle SQL_HANDLE_DBC db) db 'SQLFreeHandle)
           (YN 'disconnect (SQLFreeHandle SQL_HANDLE_ENV env) env 'SQLFreeHandle)))
        ;; Stage 3 (called in non-atomic mode):
        (void/reference-sink dont-gc)
        (let ([nos (filter no? (flatten yns))])
          (cond [(pair? nos)
                 (lambda ()
                   (void/reference-sink dont-gc)
                   (yn-refs/void nos))]
                [else void]))))

    (define/public (get-base) this)

    (define/public (free-statement pst need-lock?)
      (define (go) (free-statement* 'free-statement pst))
      (if need-lock?
          (call-with-lock* 'free-statement go go #f)
          (go)))

    (define/private (free-statement* who pst)
      (yn-refs/void
       (A* #:db db
           (let ([stmt (send pst get-handle)])
             (send pst set-handle #f)
             (hash-remove! statement-table stmt)
             (if (and stmt db)
                 (list (YN who (SQLFreeStmt stmt SQL_CLOSE) stmt 'SQLFreeStmt)
                       (YN who (SQLFreeHandle SQL_HANDLE_STMT stmt) stmt 'SQLFreeHandle))
                 null)))))

    (define/public (test:count-statements)
      (hash-count statement-table))

    ;; ----------------------------------------
    ;; Transactions

    (define/override (start-transaction* who isolation option)
      (when (eq? isolation 'nested)
        (error who "already in transaction~a"
               ";\n nested transactions not supported for ODBC connections"))
      (when option
        ;; No options supported
        (raise-argument-error who "#f" option))
      (define-values (ok-levels default-level)
        (yn-ref-values
         (A #:db db
            (yndo [(list _ ok-levels)
                   (YN who (SQLGetInfo db SQL_TXN_ISOLATION_OPTION) db)]
                  [(list _ default-level)
                   (YN who (SQLGetInfo db SQL_DEFAULT_TXN_ISOLATION) db)]
                  => (yes (list ok-levels default-level))))))
      (define requested-level
        (case isolation
          [(serializable) SQL_TXN_SERIALIZABLE]
          [(repeatable-read) SQL_TXN_REPEATABLE_READ]
          [(read-committed) SQL_TXN_READ_COMMITTED]
          [(read-uncommitted) SQL_TXN_READ_UNCOMMITTED]
          [else
           ;; MySQL ODBC returns 0 for default level, seems no good.
           ;; So if 0, use serializable.
           (if (zero? default-level) SQL_TXN_SERIALIZABLE default-level)]))
      (when (zero? (bitwise-and requested-level ok-levels))
        (error who "requested isolation level is not available\n  level: ~e"
               isolation))
      (yn-ref
       (worker-call
        (lambda (worker?)
          (define-syntax-rule (FA #:db db e ...) (FA* worker? #:db db e ...))
          (FA #:db db
              (yndo [_ (YN who (SQLSetConnectAttr db SQL_ATTR_TXN_ISOLATION requested-level) db)]
                    [_ (YN who (SQLSetConnectAttr db SQL_ATTR_AUTOCOMMIT SQL_AUTOCOMMIT_OFF) db)]
                    [#:do (set-tx-status! who #t)]
                    => (yes (void))))))))

    (define/override (end-transaction* who mode _savepoint)
      ;; _savepoint = #f, because nested transactions not supported on ODBC
      (yn-ref
       (worker-call
        (lambda (worker?)
          (define-syntax-rule (FA #:db db e ...) (FA* worker? #:db db e ...))
          (define completion-type
            (case mode
              ((commit) SQL_COMMIT)
              ((rollback) SQL_ROLLBACK)))
          (FA #:db db
              (yndo [_ (YN who (SQLEndTran db completion-type) db)]
                    [_ (YN who (SQLSetConnectAttr db SQL_ATTR_AUTOCOMMIT SQL_AUTOCOMMIT_ON) db)]
                    ;; commit/rollback can fail; don't change status until possible error handled
                    [#:do (set-tx-status! who #f)]
                    => (yes (void))))))))

    ;; ----------------------------------------
    ;; List tables

    (define/public (list-tables fsym schema)
      (define (no-search)
        (error fsym "schema search path cannot be determined for this DBMS"))
      (let ([stmt
             (cond
              [(regexp-match? #rx"^DB2" dbms)
               (let* ([schema-cond
                       (case schema
                         ((search-or-current current)
                          "tabschema = CURRENT_SCHEMA")
                         ((search)
                          (no-search)))]
                      [type-cond
                       ;; FIXME: what table types to include? see docs for SYSCAT.TABLES
                       "(type = 'T' OR type = 'V')"])
                 (string-append "SELECT tabname FROM syscat.tables "
                                "WHERE " type-cond " AND " schema-cond))]
              [(equal? dbms "Oracle")
               (let* ([schema-cond
                       (case schema
                         ((search-or-current current)
                          "owner = sys_context('userenv', 'current_schema')")
                         ((search)
                          (no-search)))])
                 (string-append "SELECT table_name AS name FROM sys.all_tables "
                                "WHERE " schema-cond
                                "UNION "
                                "SELECT view_name AS name FROM sys.all_views "
                                "WHERE " schema-cond))]
              [else
               (error fsym "not supported for this DBMS")])])
        (let* ([result (query fsym stmt #f)]
               [rows (rows-result-rows result)])
          (for/list ([row (in-list rows)])
            (vector-ref row 0)))))

    ;; ----------------------------------------
    ;; Status handler

    (define/private (handle/yn who vs handle [opname #f] [on-notice #f])
      ;; Common cases from handle/yn* pulled up to avoid on-notice alloc.
      (define status (and (pair? vs) (car vs)))
      (cond [(or (eqv? status SQL_SUCCESS)
                 (eqv? status SQL_NO_DATA))
             (yes vs)]
            [else
             (define result
               (handle/yn* who vs handle opname
                           (or on-notice
                               (lambda (sqlstate message)
                                 (add-delayed-call!
                                  (lambda () (notice-handler sqlstate message)))))))
             ;; Be careful: shouldn't do rollback before handle/yn* in case
             ;; rollback destroys statement with diagnostic records.
             (when (no? result)
               ;; On error, driver may rollback whole transaction, last statement, etc.
               ;; Options:
               ;;   1) if transaction was rolled back, set autocommit=true
               ;;   2) automatically rollback on error
               ;;   3) create flag: "transaction had error, please call rollback" (like pg)
               ;; Option 1 would be nice, but as far as I can tell, there's
               ;; no way to find out if the transaction was rolled back. And
               ;; it would be very bad to leave autocommit=false, because
               ;; that would be interpreted as "still in same transaction".
               ;; Go with (3) for now, maybe support (2) as option later.
               ;; FIXME: I worry about multi-statements like "<cause error>; commit"
               ;; if the driver does one-statement rollback.
               (when (get-tx-status)
                 (set-tx-status! who 'invalid)))
             result]))
    ))

;; ============================================================

(define shutdown-connection
  ;; Keep a reference to the class to keep all FFI callout objects
  ;; (eg, SQLDisconnect) used by its methods from being finalized.
  (let ([dont-gc connection%])
    (lambda (obj)
      (send obj real-disconnect)
      (void/reference-sink dont-gc))))

(define (handle/yn* who vs handle [opname #f] [on-notice void])
  (define status (if (pair? vs) (car vs) SQL_SUCCESS))
  (cond [(or (= status SQL_SUCCESS) (= status SQL_NO_DATA))
         (yes vs)]
        [(= status SQL_SUCCESS_WITH_INFO)
         (when (and handle (not (eq? on-notice void)))
           (define htype (handle->handle-type handle))
           (define nrecords (SQLGetDiagField/Integer htype handle 0 SQL_DIAG_NUMBER))
           (for ([i (in-range 1 (add1 nrecords))])
             (define-values (s sqlstate native-errcode message)
               (SQLGetDiagRec htype handle i))
             (on-notice sqlstate message)))
         (yes vs)]
        [(= status SQL_ERROR)
         (cond [handle
                (define htype (handle->handle-type handle))
                (define-values (s sqlstate native-errcode message)
                  (SQLGetDiagRec htype handle 1))
                (cond [(or (= s SQL_SUCCESS)
                           (= s SQL_SUCCESS_WITH_INFO))
                       (no (lambda ()
                             (raise-sql-error who sqlstate
                                              (string-append
                                               (or message "<no diagnostic message>")
                                               (if opname (format "\n  operation: ~s" opname) ""))
                                              `((code . ,sqlstate)
                                                (message . ,message)
                                                (operation . ,opname)
                                                (native-errcode . ,native-errcode)))))]
                      [else
                       (no (lambda ()
                             (error who "internal error: SQLGetDiagRec failed~a~a"
                                    (if (= s SQL_NO_DATA) "with SQL_NO_DATA" "")
                                    (if opname (format "\n  operation: ~s" opname) ""))))])]
               [else
                (no (lambda ()
                      (error who "unknown error (no diagnostic returned)")))])]
        [(= status SQL_INVALID_HANDLE)
         (no (lambda () (error who "internal error: invalid handle\n  handle: ~e" handle)))]
        [(= status SQL_NEED_DATA)
         (no (lambda () (error who "internal error: received SQL_NEED_DATA")))]
        [(= status SQL_STILL_EXECUTING)
         (no (lambda () (error who "internal error: received SQL_STILL_EXECUTING")))]
        [else
         (no (lambda () (error who "internal error: invalid result code: ~s" status)))]))

(define (handle-status* who s [handle #f])
  (yn-ref (handle/yn* who (list s) handle))
  (void))

(define (handle->handle-type handle)
  (cond [(sqlhenv? handle) SQL_HANDLE_ENV]
        [(sqlhdbc? handle) SQL_HANDLE_DBC]
        [(sqlhstmt? handle) SQL_HANDLE_STMT]
        [else #f]))

(define (marshal-decimal f n)
  (cond [(not (real? n))
         (error/no-convert f #f "numeric" n)]
        [(eqv? n +nan.0)
         "NaN"]
        [(or (eqv? n +inf.0) (eqv? n -inf.0))
         (error/no-convert f #f "numeric" n)]
        [(or (integer? n) (inexact? n))
         (number->string n)]
        [(exact? n)
         ;; Bleah.
         (or (exact->decimal-string n)
             (number->string (exact->inexact n)))]))
