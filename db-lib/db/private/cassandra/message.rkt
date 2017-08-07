#lang racket/base
(require racket/match
         racket/set
         racket/string
         racket/date
         file/sha1
         db/private/generic/interfaces
         db/private/generic/sql-data
         (only-in db/util/datetime srfi-date->sql-timestamp sql-datetime->srfi-date)
         (only-in db/util/postgresql uuid?))
(provide (all-defined-out))

;; References:
;; https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol_v3.spec
;; https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol_v4.spec

(define VERSION #x03)
(define OUT-FLAGS #x00)
(define MAX-FRAME-LENGTH (expt 2 28))

(define (invert-hash h) (for/hash ([(k v) (in-hash h)]) (values v k)))

;; ----

(define int=>opcode
  '#hash([#x00 . ERROR]
         [#x01 . STARTUP]
         [#x02 . READY]
         [#x03 . AUTHENTICATE]
         [#x05 . OPTIONS]
         [#x06 . SUPPORTED]
         [#x07 . QUERY]
         [#x08 . RESULT]
         [#x09 . PREPARE]
         [#x0A . EXECUTE]
         [#x0B . REGISTER]
         [#x0C . EVENT]
         [#x0D . BATCH]
         [#x0E . AUTH_CHALLENGE]
         [#x0F . AUTH_RESPONSE]
         [#x10 . AUTH_SUCCESS]))
(define opcode=>int (invert-hash int=>opcode))

;; ============================================================
;; Basic IO

(define (io:write-byte port byte)
  (write-byte byte port))
(define (io:read-byte port)
  (read-byte port))

(define (io:write-uint16 port val)
  (write-bytes (integer->integer-bytes val 2 #f #t) port))
(define (io:read-uint16 port)
  (integer-bytes->integer (read-bytes 2 port) #f #t))

(define (io:write-int32 port val)
  (write-bytes (integer->integer-bytes val 4 #t #t) port))
(define (io:read-int32 port)
  (integer-bytes->integer (read-bytes 4 port) #t #t))

(define (io:write-int64 port val)
  (write-bytes (integer->integer-bytes val 8 #t #t) port))
(define (io:read-int64 port)
  (integer-bytes->integer (read-bytes 8 port) #t #t))

(define (io:write-bytes port bytes)
  (write-bytes bytes port))
(define (io:read-bytes port len)
  (read-bytes len port))

;; ------------------------------------------------------------
;; Basic IO

(define (write-Int out n) (io:write-int32 out n))
(define (read-Int in) (io:read-int32 in))

(define (write-Long out n) (io:write-int64 out n))
(define (read-Long in) (io:read-int64 in))

(define (write-Short out n) (io:write-uint16 out n))
(define (read-Short in) (io:read-uint16 in))

(define (write-String out s)
  (define bs (string->bytes/utf-8 s))
  (write-Short out (bytes-length bs))
  (io:write-bytes out bs))
(define (read-String in)
  (define len (read-Short in))
  (define bs (io:read-bytes in len))
  (bytes->string/utf-8 bs))

(define (write-Symbol out s)
  (write-String (symbol->string s)))
(define (read-Symbol in)
  (string->symbol (read-String in)))

(define (write-LongString out s)
  (define bs (string->bytes/utf-8 s))
  (write-Int out (bytes-length bs))
  (io:write-bytes out bs))
(define (read-LongString in)
  (define len (read-Int in))
  (define bs (io:read-bytes in len))
  (bytes->string/utf-8 bs))

;; UUID: 16 bytes
(define (write-UUID out s)
  (io:write-bytes out (hex-string->bytes (string-replace s "-" ""))))
(define (read-UUID in)
  (define bs (io:read-bytes in 16))
  (define no-dashes (bytes->hex-string bs))
  (string-append (substring no-dashes 0 8)  "-"
                 (substring no-dashes 8 12) "-"
                 (substring no-dashes 12 16) "-"
                 (substring no-dashes 16 20) "-"
                 (substring no-dashes 20 32)))

;; [X list] = [n:short] n*[X]
(define (write-Listof out xs write-X)
  (write-Short out (length xs))
  (for ([x (in-list xs)]) (write-X out x)))
(define (read-Listof in read-X)
  (define len (read-Short in))
  (for/list ([_i (in-range len)]) (read-X in)))

(define (write-IntListof out xs write-X)
  (write-Int out (length xs))
  (for ([x (in-list xs)]) (write-X out x)))
(define (read-IntListof in read-X)
  (define len (read-Int in))
  (for/list ([_i (in-range len)]) (read-X in)))

(define (write-Bytes out bs)
  (write-Int out (if bs (bytes-length bs) -1))
  (when bs (write-bytes bs out)))
(define (read-Bytes in)
  (define len (read-Int in))
  (and (>= len 0) (io:read-bytes in len)))

;; [short bytes] doesn't make sense, no null representation
(define (write-ShortBytes out bs)
  (write-Short out (bytes-length bs))
  (io:write-bytes out bs))
(define (read-ShortBytes in)
  (define len (read-Short in))
  (io:read-bytes in len))

;; [option] = [short] [?value] (value type depends on option)

;; [inet] = [n:byte in {4,6}] n*[byte] [port:int]
(define (read-Inet in)
  (define addrlen (io:read-byte in))
  (define addr (io:read-bytes in addrlen))
  (define port (io:read-int32 in))
  (list addr port))

(define int=>consistency
  #hash([#x0000 . any]
        [#x0001 . one]
        [#x0002 . two]
        [#x0003 . three]
        [#x0004 . quorum]
        [#x0005 . all]
        [#x0006 . local-quorum]
        [#x0007 . each-quorum]
        [#x0008 . serial]
        [#x0009 . local-serial]
        [#x000a . local-one]))
(define consistency=>int (invert-hash int=>consistency))
(define consistency-symbols (hash-keys consistency=>int))

(define (write-Consistency out c)
  (write-Short out (hash-ref consistency=>int c)))
(define (read-Consistency in)
  (hash-ref int=>consistency (read-Short in)))

(define (write-StringMap out m)
  (write-Short out (length m))
  (for ([kv (in-list m)])
    (write-String out (car kv))
    (write-String out (cdr kv))))
(define (read-StringMap in)
  (define len (read-Short in))
  (for/list ([_i (in-range len)])
    (cons (read-String in)
          (read-String in))))

(define (write-StringMultiMap out mm)
  (write-Short out (length mm))
  (for ([kvs (in-list mm)])
    (write-String out (car kvs))
    (write-Listof out (cdr kvs) write-String)))
(define (read-StringMultiMap in)
  (define len (read-Short in))
  (for/list ([i (in-range len)])
    (cons (read-String in)
          (read-Listof in read-String))))

;; ------------------------------------------------------------

;; A RequestMessage is one of
;; - (Startup ProtoStringMultiMap)
;; - (AuthResponse ProtoBytes)
;; - (Options)
;; - (Query String Consistency (U #f (Listof Bytes)))
;; - (Prepare String)
;; - (Execute Bytes Consistency (U #f (Listof Bytes)))

(struct Startup (options) #:transparent)
(struct AuthResponse (response) #:transparent)
(struct Options () #:transparent)
(struct Query (query consist params) #:transparent) ;; FIXME: more parts
(struct Prepare (query) #:transparent)
(struct Execute (stmt consist params) #:transparent) ;; FIXME: more parts
;; FIXME: Batch
(struct Register (types) #:transparent)

(define the-startup (Startup '(("CQL_VERSION" . "3.0.0"))))

;; write-message : OutputPort Message Int -> Void
(define (write-message out msg streamid)
  (define body-out (open-output-bytes))
  (write-request-body body-out msg)
  (io:write-byte out (+ VERSION)) ;; + #x80 for response
  (io:write-byte out OUT-FLAGS)
  (write-Short out streamid)
  (io:write-byte out (hash-ref opcode=>int (message->opcode msg)))
  (write-Bytes out (get-output-bytes body-out))
  (flush-output out))

(define (write-request-body out msg)
  (match msg
    [(Startup options)
     (write-StringMap out options)]
    [(AuthResponse response)
     (write-Bytes out response)]
    [(Options)
     (void)]
    [(Query query consist params)
     (write-LongString out query)
     (write-Consistency out consist)
     (io:write-byte out (+ (if params #x01 0)))
     (when params (write-QueryParameters out params))]
    [(Prepare query)
     (write-LongString out query)]
    [(Execute stmt consist params)
     (write-ShortBytes out stmt)
     (write-Consistency out consist)
     (io:write-byte out (+ (if params #x01 0)))
     (when params (write-QueryParameters out params))]
    [(Register types)
     (write-Listof out types write-String)]
    ))

(define (write-QueryParameters out params)
  (write-Listof out params write-QueryParameter))
(define (write-QueryParameter out param)
  (if (sql-null? param) (write-Int out -1) (io:write-bytes out param)))

(define (message->opcode msg)
  (cond [(Startup? msg)      'STARTUP]
        [(AuthResponse? msg) 'AUTH_RESPONSE]
        [(Options? msg)      'OPTIONS]
        [(Query? msg)        'QUERY]
        [(Prepare? msg)      'PREPARE]
        [(Execute? msg)      'EXECUTE]
        [(Register? msg)     'REGISTER]))

;; ------------------------------------------------------------

;; read-response : InputPort -> (cons ResponseMessage Int)
(define (read-response in)
  (define version (io:read-byte in))
  (define flags (io:read-byte in))
  (define streamid (io:read-uint16 in))
  (define opcode (io:read-byte in))
  (define body (read-Bytes in))
  (cons (read-response-body (open-input-bytes body) (hash-ref int=>opcode opcode))
        streamid))

;; A ResponseMessage is one of
;; - (Error Integer String)
;; - (Ready)
;; - (Authenticate String)
;; - (Supported StringMultiMap)
;; - (Result:Void _)
;; - (Result:Rows _ Bytes/#f DVecs/#f (Listof Row))
;; - (Result:SetKeyspace _ String)
;; - (Result:Prepared _ Bytes DVecs/#f DVecs/#f)
;; - (Result:SchemaChange _ String String (U String (List String String)))

;; A DVecs is (Listof DVec)
;; A DVec is (vector keyspace:String table:String name:String Type)
;; A Row is (Vectorof Bytes)
(define (dvec-type dv) (vector-ref dv 3))
(define (dvec->field-info dv)
  (match dv
    [(vector keyspace table name type)
     `((keyspace . ,keyspace)
       (table . ,table)
       (name . ,name)
       (type . ,type))]))

(struct Error (code message) #:transparent)
(struct Ready () #:transparent)
(struct Authenticate (message) #:transparent)
(struct Supported (options) #:transparent)
(struct AuthChallenge (message) #:transparent)
(struct AuthSuccess (message) #:transparent)

(struct Result (kind) #:transparent)
(struct Result:Void Result () #:transparent)
(struct Result:Rows Result (pagestate info rows) #:transparent)
(struct Result:SetKeyspace Result (keyspace) #:transparent)
(struct Result:Prepared Result (stmt param-info result-info) #:transparent)
(struct Result:SchemaChange Result (change-type target details) #:transparent)

(struct Event (kind) #:transparent)
(struct Event:TopologyChange Event (change node) #:transparent)
(struct Event:StatusChange Event (change node) #:transparent)
(struct Event:SchemaChange Event (change type details) #:transparent)

(define (read-response-body in opcode)
  (case opcode
    [(ERROR)
     (Error (read-Int in)
            (read-String in))]
    [(READY)
     (Ready)]
    [(AUTHENTICATE)
     (Authenticate (read-String in))]
    [(SUPPORTED)
     (Supported (read-StringMultiMap in))]
    [(RESULT)
     (case (read-Int in)
       [(#x0001)
        (Result:Void 'VOID)]
       [(#x0002)
        (define-values (pagestate column-count info) (read-Metadata in #t))
        (define rows-count (read-Int in))
        (define rows (for/list ([i (in-range rows-count)]) (read-Row in column-count)))
        (for-each (make-decode-row! info) rows)
        (Result:Rows 'ROWS pagestate info rows)]
       [(#x0003)
        (Result:SetKeyspace 'SET_KEYSPACE (read-String in))]
       [(#x0004)
        (define stmt (read-ShortBytes in))
        (define param-info (read-Metadata in #f))
        (define result-info (read-Metadata in #f))
        (Result:Prepared 'PREPARED stmt param-info result-info)]
       [(#x0005)
        (read-SchemaChange 'SCHEMA_CHANGE in 'result)])]
    [(EVENT)
     (define kind (read-Symbol in))
     (case kind
       [(TOPOLOGY_CHANGE)
        (define change (read-String in))
        (define node (read-Inet in))
        (Event:TopologyChange kind change node)]
       [(STATUS_CHANGE)
        (define change (read-String in))
        (define node (read-Inet in))
        (Event:StatusChange kind change node)]
       [(SCHEMA_CHANGE)
        (read-SchemaChange in 'event)]
       [else (Event kind)])]
    [(AUTH_CHALLENGE)
     (AuthChallenge (read-Bytes in))]
    [(AUTH_SUCCESS)
     (AuthSuccess (read-Bytes in))]
    [else (error 'read-response-body "unknown opcode: ~e" opcode)]))

(define (read-Row in column-count)
  (define v (make-vector column-count))
  (for ([i (in-range column-count)])
    (vector-set! v i (read-Bytes in)))
  v)

(define (read-SchemaChange in make)
  (define type (read-Symbol in))
  (define target (read-Symbol in))
  (define details
    (case target
      [(KEYSPACE) (read-String in)]
      [(TABLE TYPE) (list (read-String in) (read-String in))]))
  (case make
    [(result) (Result:SchemaChange type target details)]
    [(event) (Event:SchemaChange 'SCHEMA_CHANGE type target details)]))

(define (read-Metadata in detailed?)
  (define flags (read-Int in))
  (define global-spec? (bitwise-bit-set? flags 0))
  (define more-pages? (bitwise-bit-set? flags 1))
  (define metadata? (not (bitwise-bit-set? flags 2)))
  (define column-count (read-Int in))
  (define pagestate (and more-pages? (read-Bytes in)))
  (define global-keyspace (and global-spec? metadata? (read-String in)))
  (define global-table (and global-spec? metadata? (read-String in)))
  (define column-specs
    (and metadata?
         (for/list ([i (in-range column-count)])
           (define keyspace (if global-spec? global-keyspace (read-String in)))
           (define table (if global-spec? global-table (read-String in)))
           (define name (read-String in))
           (define type (read-Type in))
           (vector keyspace table name type))))
  (if detailed?
      (values pagestate column-count column-specs)
      column-specs))

;; A Type is one of
;; - String    -- custom type
;; - Symbol    -- builtin type
;; - `(list ,Type)
;; - `(map ,Type ,Type)
;; - `(set ,Type)
;; - `(tuple ,@(Listof Type))

;; read-Type : InputPort -> (U String Symbol)
(define (read-Type in)
  (define type (read-Short in))
  (case type
    [(#x0000) (read-String in)]
    [(#x0001) 'ascii]
    [(#x0002) 'bigint]
    [(#x0003) 'blob]
    [(#x0004) 'boolean]
    [(#x0005) 'counter]
    [(#x0006) 'decimal]
    [(#x0007) 'double]
    [(#x0008) 'float]
    [(#x0009) 'int]
    [(#x000B) 'timestamp]
    [(#x000C) 'uuid]
    [(#x000D) 'varchar]
    [(#x000E) 'varint]
    [(#x000F) 'timeuuid]
    [(#x0010) 'inet]
    [(#x0020) `(list ,(read-Type in))]
    [(#x0021) `(map ,(read-Type in) ,(read-Type in))]
    [(#x0022) `(set ,(read-Type in))]
    [(#x0030) (error 'unsupported)]
    [(#x0031) `(tuple ,@(read-Listof in read-Type))]
    ))

(define (make-decode-row! info)
  (define types (map dvec-type info))
  (lambda (row)
    (for ([type (in-list types)]
          [i (in-naturals)])
      (vector-set! row i (decode-value type (vector-ref row i))))))

(define (decode-value type bs)
  (if (not bs)
      sql-null
      (match type
        ['ascii
         (bytes->string/latin-1 bs)]
        [(or 'bigint 'int)
         (integer-bytes->integer bs #t #t)]
        ['blob
         bs]
        ['boolean
         (not (equal? bs #"\0"))]
        ['decimal
         (define nexp (integer-bytes->integer bs #t #t 0 4))
         (define mantissa (base256->int (subbytes bs 4)))
         (* mantissa (expt 10 (- nexp)))]
        [(or 'double 'float)
         (floating-point-bytes->real bs #t)]
        ['inet
         (read-Inet (open-input-bytes bs))]
        [(or 'text 'varchar)
         (bytes->string/utf-8 bs)]
        ['timestamp
         (define ms (integer-bytes->integer bs #t #t))
         (srfi-date->sql-timestamp
          (seconds->date (/ ms 1000) #f))]
        [(or 'uuid 'timeuuid)
         (read-UUID (open-input-bytes bs))]
        ['varint
         (base256->int bs)]
        [`(list ,eltype)
         (map (lambda (e) (decode-value eltype e))
              (read-IntListof (open-input-bytes bs) read-Bytes))]
        [`(set ,eltype)
         (list->set
          (map (lambda (e) (decode-value eltype e))
               (read-IntListof (open-input-bytes bs) read-Bytes)))]
        [`(map ,keytype ,valtype)
         (read-IntListof (open-input-bytes bs)
                         (lambda (in)
                           (cons (decode-value keytype (read-Bytes in))
                                 (decode-value valtype (read-Bytes in)))))]
        [`(tuple ,@eltypes)
         (define in (open-input-bytes bs))
         (for/vector ([eltype (in-list eltypes)])
           (decode-value eltype (read-Bytes in)))]
        [else (error 'decode-value "unsupported type: ~e" type)])))

(define (base256->int bs)
  (define unsigned (base256->uint bs))
  (if (< (bytes-ref bs 0) #x80)
      unsigned
      (- unsigned (expt 2 (* 8 (bytes-length bs))))))

(define (base256->uint bs)
  (for/fold ([acc 0]) ([b (in-bytes bs)])
    (+ b (arithmetic-shift acc 8))))

(define (int->base256 n)
  (cond [(negative? n)
         (define absn (abs n))
         (define 2^len (let loop ([acc 128])
                         (if (<= absn acc) (* 2 acc) (loop (* acc 256)))))
         (uint->base256 (- 2^len absn) #f)]
        [else (uint->base256 n #t)]))

(define (uint->base256 n [zpad? #f])
  (define out (open-output-bytes))
  (let loop ([n n] [last-low? #f])
    (if (zero? n)
        (when (and zpad? (not last-low?))
          (write-byte 0 out))
        (let ([r (remainder n 256)])
          (loop (quotient n 256) (< r #x80))
          (write-byte r out))))
  (get-output-bytes out))

;; encode-value : Symbol Type Any -> Bytes
(define (encode-value who type0 v0)
  (define (err)
    (error/no-convert who "Cassandra" type0 v0 #:contract (type->contract-sexp type0)))
  (define (encode type v)
    (cond [(sql-null? v)
           (integer->integer-bytes -1 4 #t #t)]
          [else
           (define bs (encode* type v))
           (bytes-append (integer->integer-bytes (bytes-length bs) 4 #t #t) bs)]))
  (define (encode* type v)
    (match type
      ['ascii
       (unless (and (string? v) (regexp-match #px"^[[:ascii:]]*$" v)) (err))
       (string->bytes/latin-1 v)]
      ['bigint
       (unless (int64? v) (err))
       (integer->integer-bytes v 8 #t #t)]
      ['int
       (unless (int32? v) (err))
       (integer->integer-bytes v 4 #t #t)]
      ['blob
       (unless (bytes? v) (err))
       v]
      ['boolean
       (case v
         [(#t) #"\1"]
         [(#f) #"\0"]
         [else (err)])]
      ['decimal
       (error 'encode-value "unsupported type: ~e" type)]
      ['double
       (unless (real? v) (err))
       (real->floating-point-bytes v 8 #t)]
      ['float
       (unless (real? v) (err))
       (real->floating-point-bytes v 4 #t)]
      ['inet
       (error 'encode-value "unsupported type: ~e" type)]
      [(or 'text 'varchar)
       (unless (string? v) (err))
       (string->bytes/utf-8 v)]
      ['timestamp
       (unless (or (sql-timestamp? v) (date? v)) (err))
       (define d (if (sql-timestamp? v) (sql-datetime->srfi-date v) v))
       (define ms (inexact->exact (round (* 1000 (date*->seconds v)))))
       (integer->integer-bytes ms 8 #t #t)]
      [(or 'uuid 'timeuuid)
       (unless (uuid? v) (err))
       (define out (open-output-bytes))
       (write-UUID v)
       (get-output-bytes out)]
      ['varint
       (unless (exact-integer? v) (err))
       (int->base256 v)]
      [`(list ,eltype)
       (unless (list? v) (err))
       (let ([b (apply bytes-append (map (lambda (el) (encode eltype el)) v))])
         (bytes-append (integer->integer-bytes (length v) 4 #t #t) b))]
      [`(set ,eltype)
       (unless (set? v) (err))
       (let ([b (apply bytes-append (map (lambda (el) (encode eltype el)) (set->list v)))])
         (bytes-append (integer->integer-bytes (set-count v) 4 #t #t) b))]
      [`(map ,keytype ,valtype)
       (unless (and (list? v) (andmap pair? v)) (err))
       (define (encode-pair p) (bytes-append (encode keytype (car p)) (encode valtype (cdr p))))
       (let ([b (apply bytes-append (map encode-pair v))])
         (bytes-append (integer->integer-bytes (length v) 4 #t #t) b))]
      [`(tuple ,@eltypes)
       (unless (and (vector? v) (= (vector-length v) (length eltypes))) (err))
       (apply bytes-append
              (for/list ([eltype (in-list eltypes)] [el (in-vector v)])
                (encode eltype el)))]
      [else (error 'encode-value "unsupported type: ~e" type)]))
  (encode type0 v0))

(define (type->contract-sexp type)
  (let loop ([type type])
    (match type
      ['ascii '(and/c string? #px"^[[:ascii:]]*$")]
      ['bigint 'int64?]
      ['int 'int32?]
      ['blob 'bytes?]
      ['boolean 'boolean?]
      ['decimal '(and/c real? exact?)]
      ['double 'real?]
      ['float 'real?]
      ;; ['inet ???]
      ['text 'string?]
      ['varchar 'string?]
      ['timestamp 'date?]
      ['uuid 'uuid?]
      ['timeuuid 'uuid?]
      ['varint 'exact-integer?]
      [`(list ,eltype) `(listof ,(loop eltype))]
      [`(set ,eltype) `(setof ,(loop eltype))]
      [`(map ,ktype ,vtype) `(listof (cons/c ,(loop ktype) ,(vtype)))]
      [`(tuple ,@eltypes) `(vector ,@(map loop eltypes))]
      [_ (error 'encode-value "unsupported type: ~e" type)])))
