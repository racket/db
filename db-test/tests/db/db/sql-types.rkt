#lang racket/unit
(require rackunit
         racket/class
         racket/list
         racket/math
         racket/match
         racket/string
         racket/flonum
         (prefix-in srfi: srfi/19)
         db/base
         db/util/datetime
         db/util/geometry
         db/util/postgresql
         db/util/mysql
         "../config.rkt")
(import config^ database^)
(export test^)

;; ----------------------------------------

(define dbc (make-parameter #f))
(define current-rtconf (make-parameter #f))

;; ----------------------------------------
;; Roundtripping

;; tc : RTConf (-> Void) ... -> Void (TestCase)
(define (tc cnf . testers)
  (test-case (format "~a" (rtconf-name cnf))
    (call-with-connection
     (lambda (c)
       (parameterize ((dbc c) (current-rtconf cnf))
         ((rtconf-setup cnf))
         (for ([tester (in-list testers)])
           (tester))))
     #f)))

(struct rtconf
  (name                 ;; Symbol
   type                 ;; String
   setup                ;; -> Void
   check-rt-param       ;; Value -> Void
   check-rt-table)      ;; Value -> Void
  #:prefab)

;; mkconf : ... -> RTConf
;; If param = #f, don't rt via param.
;; If coltype = #f, don't rt via table.
(define (mkconf type-symbol
                #:type    [type (format "~a" type-symbol)]
                #:cast    [cast-type type]
                #:param   [param-sql (if cast-type
                                         (if (FLAG 'postgresql)
                                             (format "$1::~a" cast-type)
                                             (format "cast($1 as ~a)" cast-type))
                                         "$1")]
                #:coltype [column-type type]
                #:insert-column [insert-column-sql "$1"]
                #:select-column [select-column-fmt "~a"]
                #:wrap    [wrap values]
                #:check   [check check-equal?])
  (define check-rt-param
    (cond [param-sql
           (define select-sql (select-val param-sql))
           (define (check-rt-param value)
             (check (query-value (dbc) select-sql (wrap value))
                    value
                    "rt through param"))
           check-rt-param]
          [else void]))
  (define-values (rt-table drop-rt-table-sql create-rt-table-fmt)
    (cond [(not column-type) (values #f #f #f)]
          [(FLAG 'can-temp-table)
           (values "test_rt_table" #f
                   "create temporary table test_rt_table (v ~a)")]
          ;; None of these work.
          ;; [(FLAG 'ismss)
          ;;  (values "#test_rt_table" #f
          ;;          "create table #test_rt_table (v ~a)")]
          ;; [(FLAG 'isora) ___]
          ;; [(FLAG 'isdb2)
          ;;  (values "SESSION.test_rt_table" #f
          ;;          "create global temporary table SESSION.test_rt_table (v ~a)")]
          [(FLAG 'rt-table)
           (values "test_rt_table"
                   "drop table test_rt_table"
                   "create table test_rt_table (v ~a)")]
          [else (values #f #f #f)]))
  (define create-rt-table
    (cond [rt-table
           (lambda ()
             (when drop-rt-table-sql
               (with-handlers ([exn:fail:sql? void])
                 (query-exec (dbc) drop-rt-table-sql)))
             (query-exec (dbc) (format create-rt-table-fmt column-type)))]
          [else void]))
  (define check-rt-table
    (cond [rt-table
           (define delete-sql (sql (format "delete from ~a" rt-table)))
           (define insert-sql
             (sql (format "insert into ~a (v) values (~a)" rt-table insert-column-sql)))
           (define select-column-sql (sql (format select-column-fmt "v")))
           (define select-sql (format "select ~a from ~a" select-column-sql rt-table))
           (define (check-rt-table value)
             (query-exec (dbc) delete-sql)
             (query-exec (dbc) insert-sql (wrap value))
             (check (query-value (dbc) select-sql) value "rt through table"))
           check-rt-table]
          [else void]))
  (rtconf type-symbol
          type
          create-rt-table
          check-rt-param
          check-rt-table))

;; ----------------------------------------
;; Roundtrip checks

(define (check-rt . vs)
  (check-rts vs))

(define (check-rts vs)
  (define (shorten-value v)
    (cond [(and (bytes? v) (> (bytes-length v) 100)) (print-short v)]
          [(and (string? v) (> (string-length v) 100)) (print-short v)]
          [else v]))
  (define cnf (current-rtconf))
  (define check-rt-param (rtconf-check-rt-param cnf))
  (define check-rt-table (rtconf-check-rt-table cnf))
  (for ([v (in-list vs)])
    (with-check-info (('value-to-rt (shorten-value v)))
      (check-rt-param v)
      (check-rt-table v))))

(struct print-short (v)
  #:property prop:custom-write
  (lambda (self out mode)
    (define v (print-short-v self))
    (cond [(string? v)
           (fprintf out "~.s (~s chars)" v (string-length v))]
          [(bytes? v)
           (fprintf out "~.s (~s bytes)" v (bytes-length v))]
          [else (fprintf out "~.s" v)])))

;; ----------------------------------------
;; Other checks

(define (check-value/text v+t-list [check-value=? check-equal?] [check-text=? check-equal?])
  (cond [(FLAG 'postgresql)
         (define typename (rtconf-type (current-rtconf)))
         (define q-text->val (format "select ($1::text)::~a" typename))
         (define q-val->text (format "select ($1::~a)::text" typename))
         (for ([v+t (in-list v+t-list)])
           (match-define (list val text) v+t)
           (check-value=? (query-value (dbc) q-text->val text) val
                          "check text->value")
           (check-text=? (query-value (dbc) q-val->text val) text
                         "check value->text"))]
        [else (void)]))

(define-check (check-trim-string=? a b)
  (check-equal? (string-trim a #:left? #f) (string-trim b #:left? #f)))

(define-check (check-timestamptz-equal? a b)
  (cond [(and (sql-timestamp? a) (sql-timestamp? b))
         (check srfi:time=?
                (srfi:date->time-utc (sql-datetime->srfi-date a))
                (srfi:date->time-utc (sql-datetime->srfi-date b)))]
        [(and (pg-range? a) (pg-range? b))
         (match (list a b)
           [(list (pg-range alb ali? aub aui?) (pg-range blb bli? bub bui?))
            (and (check-timestamptz-equal? alb blb)
                 (check-equal? ali? bli?)
                 (check-timestamptz-equal? aub bub)
                 (check-equal? aui? bui?))])]
        [else (check-equal? a b)]))

(define-check (check-bits-equal? a b)
  (check-equal? (sql-bits->string a) (sql-bits->string b)))

;; ============================================================
;; Common test data

(define NR 10)
(define (random-list proc)
  (for/list ([i (in-range NR)]) (proc)))
(define (random-uint nbits)
  (cond [(zero? nbits) 0]
        [(< nbits 16)
         (random (arithmetic-shift 1 nbits))]
        [else
         (bitwise-ior
          (arithmetic-shift (random-uint (- nbits 16)) 16)
          (random (arithmetic-shift 1 16)))]))

(define (t-boolean)
  (check-rts '(#t #f)))

;; ----------------------------------------
;; Numeric types

(define (mk-int-list n)
  (define (random-int)
    (- (random-uint n) (arithmetic-shift 1 (sub1 n))))
  (define lo (- (arithmetic-shift 1 (sub1 n))))
  (define hi (sub1 (arithmetic-shift 1 (sub1 n))))
  (list* 0 1 -1 2 lo hi (random-list random-int)))

(define (mk-uint-list n)
  (define hi (sub1 (arithmetic-shift 1 n)))
  (list* 0 1 2 hi (random-list (lambda () (random-uint n)))))

(define ((tt-int n))
  (check-rts (mk-int-list n)))
(define ((tt-uint n))
  (check-rts (mk-uint-list n)))

(define (t-float4)
  (check-rts '(1.0 1.5 -5.5))
  (when (send dbsystem has-support? 'real-infinities)
    (check-rts '(+inf.0 -inf.0 +nan.0))))
(define (t-float8)
  (t-float4)
  (check-rts (random-list random)))

(define (t-numeric/30/10)
  (check-rts (list 0 10 -5 1234567890 -1234567890))
  (check-rts (list 1/2 1/40 1/10 -1/2 -1/40 -1/10 1/400000))
  (check-rts (list #e12345.67809 #e-12345.67809))
  (check-rts (list 12345678901234567890 -12345678901234567890))
  (check-rts (list #e1234567890.1234567809 #e-1234567890.1234567809))
  (when (send dbsystem has-support? 'numeric-infinities)
    (check-rts '(+inf.0 -inf.0 +nan.0))))

(define (t-numeric-unbounded)
  (t-numeric/30/10))

;; ----------------------------------------
;; Character strings

(define some-ascii-strings
  `("Az0"
    "  abc123  "
    "a\te\ni\ro\vu"
    "it's like that (and that's the way it is)"
    ,(string #\\)
    ,(string #\\ #\\)
    ,(string #\')
    ,(string #\\ #\')))
(define some-bmp-strings
  '("λ the ultimate ∀♫"
    "αβψδεφγηιξκλμνοπρστθωςχυζ"
    "नाच न जाने आँगन टेढ़ा"
    "абцдефгхиклмнопљрстувњџзѕЋч"
    "就是几句话"))
(define some-non-bmp-strings
  '("😃🚀 →↕⇐⇶ 𝄞 𝕽𝕬𝕮𝕶𝕰𝕿 𝕎𝕏"))
(define some-long-ascii-strings
  (list (make-string 100 #\a)
        (make-string 800 #\a)))
(define some-long-bmp-strings
  (list (make-string 100 #\λ)
        (build-string 100 (lambda (n) (integer->char (add1 n))))
        (make-string 800 #\u2200)
        (build-string 800 (lambda (n) (integer->char (add1 n))))))

(define (t-char1)
  (check-rts '(#\a #\Z #\7 #\space #\newline)))

(define (t-char/10)
  ;; FIXME!
  (void))

(define (t-nchar/10)
  ;; FIXME!
  (void))

;; tt-string : (U Nat #f) (U Nat #f) (U Nat 'utf8 #f) -> Void
;; Prefer [LONG #f] over [#f LONG].
(define ((tt-string maxchars maxbytes [wcharsize0 #f] #:cs [cs 'all]))
  (define wcharsize (or wcharsize0 (case cs [(ascii) 1] [(bmp) 2] [else 4])))
  (define (if-cs ok-cs xs) (if (memq cs ok-cs) xs null))
  ;; Oracle treats empty string as NULL (!)
  (unless (FLAG 'isora) (check-rt ""))
  (check-rts (filter (mk-string-ok? maxchars maxbytes wcharsize)
                     (append some-ascii-strings
                             (if-cs '(bmp all) some-bmp-strings)
                             (if-cs '(all) some-non-bmp-strings)
                             some-long-ascii-strings
                             (if-cs '(bmp all) some-long-bmp-strings))))
  (for ([n (in-range 3 7)]
        ;; Note extra factor of 10
        #:when (< (expt 10 (add1 n)) (or maxchars maxbytes +inf.0)))
    (check-rt (make-string (expt 10 n) #\x)))
  (let ([len (min (or maxchars VLONG)
                  (case wcharsize
                    [(utf8) (or maxbytes VLONG)]
                    [else (quotient (or maxbytes VLONG) wcharsize)]))])
    (check-rt (make-string len #\a))))

(define ((mk-string-ok? maxchars maxbytes wcharsize) s)
  (and (<= (string-length s) (or maxchars +inf.0))
       (case wcharsize
         [(utf8) (<= (string-utf-8-length s) (or maxbytes +inf.0))]
         [else (<= (* (string-length s) wcharsize) (or maxbytes +inf.0))])))

(define LONG #e1e6)
(define VLONG #e1e7)

;; ----------------------------------------
;; Binary strings

(define (t-bytes/10)
  ;; FIXME!
  (void))

(define ((tt-bytes maxlen))
  (check-rt #"hello") ;; assume maxlen >= 5
  (unless (ORFLAGS 'isora 'isfbd) (check-rt #""))
  (check-rt (list->bytes (build-list (min maxlen 256) values)))
  (for ([n (in-range 3 7)] #:when (< (expt 10 (add1 n)) maxlen))
    (check-rt (make-bytes (expt 10 n) (char->integer #\x))))
  (check-rt (make-bytes maxlen (char->integer #\a))))

;; ----------------------------------------
;; Dates and times

;; Note: some datetime value lists are used by PostgreSQL range tests,
;; adjust if updated or reordered.

(define some-dates
  `((,(sql-date 1776 07 04) "1776-07-04")
    (,(sql-date 2000 01 01) "2000-01-01")
    (,(sql-date 2012 02 14) "2012-02-14")))

(define some-times/no-frac-sec
  `((,(sql-time 01 02 03 0 #f) "01:02:03")
    (,(sql-time 12 34 56 0 #f) "12:34:56")
    (,(sql-time 17 30 01 0 #f) "17:30:01")))
(define some-times/with-frac-sec
  `((,(sql-time 01 02 03 #e4e7 #f) "01:02:03.04")
    (,(sql-time 12 34 56 123456000 #f) "12:34:56.123456")))

(define some-timetzs/no-frac-sec
  `((,(sql-time 01 02 03 0 3600) "01:02:03+01")
    (,(sql-time 12 34 56 0 3600) "12:34:56+01")
    (,(sql-time 17 30 01 0 -7200) "17:30:01-02")))
(define some-timetzs/with-frac-sec
  `((,(sql-time 01 02 03 #e4e7 3600) "01:02:03.04+01")
    (,(sql-time 12 34 56 123456000 3600) "12:34:56.123456+01")
    (,(sql-time 12 34 56 123456000 -7200) "12:34:56.123456-02")))

(define some-timestamps/no-frac-sec
  `((,(sql-timestamp 1776 07 04 12 34 56 0 #f) "1776-07-04 12:34:56")
    (,(sql-timestamp 2000 01 01 12 34 56 0 #f) "2000-01-01 12:34:56")
    (,(sql-timestamp 2012 02 14 12 34 56 0 #f) "2012-02-14 12:34:56")))
(define some-timestamps/with-frac-sec
  `((,(sql-timestamp 1776 07 04 12 34 56 123456000 #f) "1776-07-04 12:34:56.123456")
    (,(sql-timestamp 2000 01 01 12 34 56 123456000 #f) "2000-01-01 12:34:56.123456")
    (,(sql-timestamp 2012 02 14 12 34 56 123456000 #f) "2012-02-14 12:34:56.123456")))
(define some-pg-timestamps
  `((-inf.0 "-infinity")
    (+inf.0 "infinity")))

(define some-timestamptzs
  `((,(sql-timestamp 1776 07 04 12 34 56 0 -14400) "1776-07-04 12:34:56-04")
    (,(sql-timestamp 2000 01 01 12 34 56 0 -14400) "2000-01-01 12:34:56-04")
    (,(sql-timestamp 2012 02 14 12 34 56 0 7200) "2012-02-14 12:34:56+02")
    (,(sql-timestamp 2000 01 01 12 34 56 123456000 14400) "2000-01-01 12:34:56.123456+04")
    (,(sql-timestamp 1776 07 04 12 34 56 123456000 -14400) "1776-07-04 12:34:56.123456-04")
    (,(sql-timestamp 2012 02 14 12 34 56 123456000 -7200) "2012-02-14 12:34:56.123456-02")
    (-inf.0 "-infinity")
    (+inf.0 "infinity")))

(define frac-seconds-ok?
  ;; FIXME?
  (FLAG 'postgresql))

(define (t-date)
  (check-rts (map car some-dates))
  (check-value/text some-dates))

(define (t-time)
  (check-rts (map car some-times/no-frac-sec))
  (check-value/text some-times/no-frac-sec)
  (when frac-seconds-ok?
    (check-rts (map car some-times/with-frac-sec))
    (check-value/text some-times/with-frac-sec)))

(define (t-time+tz)
  (check-rts (map car some-timetzs/no-frac-sec))
  (check-value/text some-timetzs/no-frac-sec)
  (when frac-seconds-ok?
    (check-rts (map car some-timetzs/with-frac-sec))
    (check-value/text some-timetzs/with-frac-sec)))

(define (t-timestamp)
  (check-rts (map car some-timestamps/no-frac-sec))
  (check-value/text some-timestamps/no-frac-sec)
  (when frac-seconds-ok?
    (check-rts (map car some-timestamps/with-frac-sec))
    (check-value/text some-timestamps/with-frac-sec))
  (when (FLAG 'postgresql) ;; only PostgreSQL supports +/inf.0
    (check-rts (map car some-pg-timestamps))
    (check-value/text some-pg-timestamps)))

(define (t-timestamptz)
  (check-rts (map car some-timestamptzs))
  (check-value/text some-timestamptzs check-timestamptz-equal? void))

;; ----------------------------------------
;; Dates/time intervals

(define some-day-time-intervals
  `((,(sql-interval 0 0 3 4 5 6 0) "3 days 04:05:06")
    (,(sql-interval 0 0 -3 -4 -5 -6 0) "-3 days -04:05:06")))
(define some-year-month-intervals
  `((,(sql-interval 87 1 0 0 0 0 0) "87 years 1 mon")
    (,(sql-interval -87 -1 0 0 0 0 0) "-87 years -1 mons")))
(define some-general-intervals
  `((,(sql-interval 1 2 3 4 5 6 45000) "1 year 2 mons 3 days 04:05:06.000045")
    (,(sql-interval -1 -2 3 4 5 6 45000) "-1 years -2 mons +3 days 04:05:06.000045")))

(define (t-day-time-interval)
  (check-rts (map car some-day-time-intervals))
  (check-value/text some-day-time-intervals))

(define (t-year-month-interval)
  (check-rts (map car some-year-month-intervals))
  (check-value/text some-day-time-intervals))

(define (t-pg-interval)
  (t-day-time-interval)
  (t-year-month-interval)
  (check-rts (map car some-general-intervals))
  (check-value/text some-general-intervals))

;; ----------------------------------------
;; Geometry

(define (t-geom-point)
  (check-rt (point 0 0)
            (point 1 2)
            (point (exp 1) pi)))

(define (t-geom-line-string)
  (check-rt (line-string (list (point 0 0) (point 1 1)))
            (line-string (list (point 0 0) (point 1 1) (point -5 7)))))

(define (t-geom-polygon)
  (check-rt (polygon (line-string (list (point 0 0) (point 2 0) (point 1 1) (point 0 0))) '()))
  (when (FLAG 'mysql)
    (check-rt
     (polygon (line-string (list (point 0 0) (point 4 0) (point 2 2) (point 0 0)))
              (list (line-string (list (point 1 1) (point 3 1) (point 1.5 1.5) (point 1 1))))))))

(define (t-pg-lseg)
  (check-rt (line-string (list (point 0 0) (point 1 1)))))

(define (t-pg-box)
  (check-rt (pg-box (point 10 10) (point 2 8))))

(define (t-pg-path)
  (check-rt (pg-path #t (list (point 0 0) (point 1 1) (point -5 7)))
            (pg-path #f (list (point -1 -1) (point (exp 1) pi)))))

(define (t-pg-circle)
  (check-rt (pg-circle (point 1 2) 45)))

;; ----------------------------------------
;; Misc types

(define some-atomic-jsexprs
  (list #t #f 0 1 -2 pi "" "hello" "good\nbye" 'null))

(define some-compound-jsexprs
  (list (hasheq)
        (list)
        (hasheq 'a 1 'b 2 'c 'null 'd "apple")
        (list #t #f 'null "a" "b" (hasheq 'a 1))))

(define (t-json)
  (check-rt some-atomic-jsexprs)
  (check-rt some-compound-jsexprs))

;; ============================================================

(define (test-postgresql)

  (tc (mkconf 'smallint) (tt-int 16))
  (tc (mkconf 'integer)  (tt-int 32))
  (tc (mkconf 'bigint)   (tt-int 64))
  (tc (mkconf 'float4)   t-float4)
  (tc (mkconf 'float8)   t-float8)
  (tc (mkconf 'numeric)  t-numeric-unbounded)
  ;; DECIMAL alias for NUMERIC
  ;; SERIAL, {SMALL,BIG}SERIAL
  ;; MONEY

  (tc (mkconf 'char1 #:type "\"char\"") t-char1)
  (tc (mkconf 'char  #:type "char(10)") t-char/10)
  (tc (mkconf 'varchar)       (tt-string #f LONG))
  (tc (mkconf 'text)          (tt-string #f LONG))
  (tc (mkconf 'bytea)         (tt-bytes LONG))
  ;; BPCHAR does not trim trailing spaces; only "considered insignificant"

  (test-case "char to/from integer"
    (call-with-connection
     (lambda (dbc)
       ;; PostgreSQL ascii fun returns unicode code point too
       (define (pg:char->integer c)
         (query-value dbc "select ascii($1)" (string c)))
       (define (pg:integer->char n)
         (query-value dbc "select chr($1)" n))
       (for ([s (in-list some-bmp-strings)] #:when (positive? (string-length s)))
         (define c (string-ref s 0))
         (check-equal? (pg:char->integer c) (char->integer c))
         (check-equal? (pg:integer->char (char->integer c)) (string c)))
       (void))
     #f))

  (unless (ORFLAGS 'ssl 'benchmark)
    (test-case "large bytea"
      (call-with-connection
       (lambda (dbc)
         (define r (query-value dbc "select cast(repeat('a', 10000000) as bytea)"))
         (check-pred bytes? r)
         (check-equal? r (make-bytes #e1e7 (char->integer #\a))))
       #f)))

  (tc (mkconf 'date)          t-date)
  (tc (mkconf 'time)          t-time)
  (tc (mkconf 'timetz)        t-time+tz)
  (tc (mkconf 'timestamp)     t-timestamp)
  (tc (mkconf 'timestamptz #:check check-timestamptz-equal?) t-timestamptz)

  (tc (mkconf 'point)         t-geom-point)
  (tc (mkconf 'lseg)          t-pg-lseg)
  (tc (mkconf 'box)           t-pg-box)
  (tc (mkconf 'path)          t-pg-path)
  (tc (mkconf 'polygon)       t-geom-polygon)
  (tc (mkconf 'circle)        t-pg-circle)

  (tc (mkconf 'boolean)       t-boolean)

  (tc (mkconf 'varbit #:check check-bits-equal?)
      (lambda ()
        (check-rt (string->sql-bits "1011")
                  (string->sql-bits "000000")
                  (string->sql-bits (make-string 30 #\1))
                  (string->sql-bits
                   (string-append (make-string 10 #\1) (make-string 20 #\0))))))

  (tc (mkconf 'uuid)
      (lambda ()
        (check-rt "84cd6cee-edd0-4701-9618-d0ef84cee55d")))

  ;; BIT(n)
  ;; XML
  ;; records

  ;; ----------------------------------------

  (define-check (check-=any elt lst in?)
    (check-equal? (query-value (dbc)
                    (format "select ~a = any ($1)" elt) (list->pg-array lst))
                  in?))

  (tc (mkconf 'boolean-array #:type "boolean[]")
      (lambda ()
        (check-rt (list->pg-array (list))
                  (list->pg-array (list #t))
                  (list->pg-array (list #t #t #f))
                  (list->pg-array (list #t sql-null #f)))
        (check-=any "'t'::boolean" (list) #f)
        (check-=any "'t'::boolean" (list #f) #f)
        (check-=any "'t'::boolean" (list sql-null #t #f) #t)))

  (tc (mkconf 'bytea-array #:type "bytea[]")
      (lambda ()
        (check-rt (list->pg-array (list))
                  (list->pg-array (list #"abc"))
                  (list->pg-array (list #"a" #"bc" #"def"))
                  (list->pg-array (list #"a" sql-null #"bc")))
        (check-=any "'abc'::bytea" (list) #f)
        (check-=any "'abc'::bytea" (list #"a" #"abc") #t)
        (check-=any "'abc'::bytea" (list #"a" sql-null #"abc") #t)))

  (tc (mkconf 'char1-array #:type "\"char\"[]")
      (lambda ()
        (check-rt (list->pg-array (list))
                  (list->pg-array (list #\a))
                  (list->pg-array (list #\a #\z))
                  (list->pg-array (list #\a sql-null #\z)))
        (check-=any "'a'::\"char\"" (list) #f)
        (check-=any "'a'::\"char\"" (list #\a #\b) #t)
        (check-=any "'a'::\"char\"" (list #\a sql-null #\z) #t)))

  (tc (mkconf 'smallint-array #:type "smallint[]")
      (lambda ()
        (check-rt (list->pg-array (list))
                  (list->pg-array (list 1))
                  (list->pg-array (list 1 2 3))
                  (list->pg-array (list 1 sql-null 3 4 5)))
        (check-=any "1::smallint" (list) #f)
        (check-=any "1::smallint" (list 1 2 3) #t)
        (check-=any "1::smallint" (list sql-null 1 2 3) #t)))

  (tc (mkconf 'integer-array #:type "integer[]")
      (lambda ()
        (check-rt (list->pg-array (list))
                  (list->pg-array (list 1))
                  (list->pg-array (list 1 2 3))
                  (list->pg-array (list 1 sql-null 3 4 5)))
        (check-=any "1" (list) #f)
        (check-=any "1" (list 1 2 3) #t)
        (check-=any "1" (list sql-null 1 2 3) #t)))

  (tc (mkconf 'text-array #:type "text[]")
      (lambda ()
        (check-rt (list->pg-array (list))
                  (list->pg-array (list "abc"))
                  (list->pg-array (list "a" "" "bc" "def" ""))
                  (list->pg-array (list "a" sql-null "")))
        (check-=any "'abc'" (list) #f)
        (check-=any "'abc'" (list "abc" "def") #t)
        (check-=any "'abc'" (list "abc" "def" sql-null) #t)))

  (tc (mkconf 'varchar-array #:type "varchar[]")
      (lambda ()
        (check-rt (list->pg-array (list))
                  (list->pg-array (list "abc"))
                  (list->pg-array (list "a" "" "bc" "def" ""))
                  (list->pg-array (list "a" sql-null "")))
        (check-=any "'abc'" (list) #f)
        (check-=any "'abc'" (list "abc" "def") #t)
        (check-=any "'abc'" (list "abc" "def" sql-null) #t)))

  (tc (mkconf 'bigint-array #:type "bigint[]")
      (lambda ()
        (check-rt (list->pg-array (list))
                  (list->pg-array (list 1))
                  (list->pg-array (list 1 2 3))
                  (list->pg-array (list 1 sql-null 3 4 5)))
        (check-=any "1::bigint" (list) #f)
        (check-=any "1::bigint" (list 1 2 3) #t)
        (check-=any "1::bigint" (list sql-null 1 2 3) #t)))

  (tc (mkconf 'point-array #:type "point[]")
      (lambda ()
        (check-rt (list->pg-array (list))
                  (list->pg-array (list (point 1 2)))
                  (list->pg-array (list (point 1 2) sql-null (point 3 4))))
        #| ;; no op= for point ?!
        (check-=any "POINT (1,2)" (list (point 1 2)) #t)
        (check-=any "POINT (1,2)" (list (point 3 4)) #f)
        (check-=any "POINT (1,2)" (list sql-null (point 1 2) (point 3 4)) #t)
        |#))

  (tc (mkconf 'real-array #:type "float4[]")
      (lambda ()
        (check-rt (list->pg-array (list))
                  (list->pg-array (list 1.0))
                  (list->pg-array (list 1.0 2.0 3.0))
                  (list->pg-array (list 1.0 sql-null 3.0 4.0 +inf.0)))
        (check-=any "1.0::real" (list) #f)
        (check-=any "1.0::real" (list 1.0) #t)
        (check-=any "1.0::real" (list sql-null 1.0 2.0 +inf.0) #t)))

  (tc (mkconf 'double-array #:type "float8[]")
      (lambda ()
        (check-rt (list->pg-array (list))
                  (list->pg-array (list 1.0))
                  (list->pg-array (list 1.0 2.0 3.0))
                  (list->pg-array (list 1.0 sql-null 3.0 4.0 +inf.0)))
        (check-=any "1.0::float8" (list) #f)
        (check-=any "1.0::float8" (list 1.0) #t)
        (check-=any "1.0::float8" (list sql-null 1.0 2.0 +inf.0) #t)))

  (tc (mkconf 'timestamp-array #:type "timestamp[]")
      (lambda ()
        (define ts1 (sql-timestamp 1999 12 31 11 22 33 0 #f))
        (define ts2 (sql-timestamp 2011 09 13 15 17 19 0 #f))
        (check-rt (list->pg-array (list))
                  (list->pg-array (list ts1))
                  (list->pg-array (list ts2 sql-null)))
        (check-=any "'1999-12-31 11:22:33'::timestamp" (list) #f)
        (check-=any "'1999-12-31 11:22:33'::timestamp" (list ts2) #f)
        (check-=any "'1999-12-31 11:22:33'::timestamp" (list ts1 sql-null ts2) #t)))

  ;; FIXME: add timestamptz-array test (harder due to tz conversion)

  (tc (mkconf 'date-array #:type "date[]")
      (lambda ()
        (define ts1 (sql-date 1999 12 31))
        (define ts2 (sql-date 2011 09 13))
        (check-rt (list->pg-array (list))
                  (list->pg-array (list ts1))
                  (list->pg-array (list ts2 sql-null)))
        (check-=any "'1999-12-31'::date" (list) #f)
        (check-=any "'1999-12-31'::date" (list ts2) #f)
        (check-=any "'1999-12-31'::date" (list ts1 sql-null ts2) #t)))

  (tc (mkconf 'time-array #:type "time[]")
      (lambda ()
        (define ts1 (sql-time 11 22 33 0 #f))
        (define ts2 (sql-time 15 17 19 0 #f))
        (check-rt (list->pg-array (list))
                  (list->pg-array (list ts1))
                  (list->pg-array (list ts2 sql-null)))
        (check-=any "'11:22:33'::time" (list) #f)
        (check-=any "'11:22:33'::time" (list ts2) #f)
        (check-=any "'11:22:33'::time" (list ts1 sql-null ts2) #t)))

  ;; FIXME: add timetz-array test
  ;; FIXME: add interval-array test

  (tc (mkconf 'interval-array #:type "interval[]")
      (lambda ()
        (define i1 (sql-interval 0 0 3 4 5 6 0))
        (define i2 (sql-interval 87 1 0 0 0 0 0))
        (define i3 (sql-interval 1 2 3 4 5 6 45000))
        (check-rt (list->pg-array (list))
                  (list->pg-array (list i1))
                  (list->pg-array (list i2 sql-null i3)))
        (check-=any "'87 years 1 month'::interval" (list) #f)
        (check-=any "'87 years 1 month'::interval" (list i1 i3) #f)
        (check-=any "'87 years 1 month'::interval" (list i1 sql-null i2) #t)))

  (tc (mkconf 'decimal-array #:type "decimal[]")
      (lambda ()
        (define d1 12345678901234567890)
        (define d2 1/20)
        (define d3 +nan.0)
        (define d4 -100)
        (check-rt (list->pg-array (list))
                  (list->pg-array (list d1))
                  (list->pg-array (list d2 sql-null d3 d4)))
        (check-=any "'0.05'::numeric" (list) #f)
        (check-=any "'0.05'::numeric" (list d1) #f)
        (check-=any "'0.05'::numeric" (list d1 sql-null d2 d3 d4) #t)))

  ;; ----------------------------------------

  (when (FLAG 'pg92)
    (tc (mkconf 'json) t-json)

    ;; for now, only test ranges in canonical form... (FIXME)
    (tc (mkconf 'int4range)
        (lambda ()
          (check-rt (pg-empty-range)
                    (pg-range 0 #t 5 #f)
                    (pg-range #f #f -57 #f)
                    (pg-range 1234 #t #f #f))))
    (tc (mkconf 'int8range)
        (lambda ()
          (check-rt (pg-empty-range)
                    (pg-range 0 #t 5 #f)
                    (pg-range #f #f -57 #f)
                    (pg-range 1234 #t #f #f)
                    (pg-range (expt 2 60) #t (expt 2 61) #f))))
    ;; FIXME: numrange
    (tc (mkconf 'daterange)
        (lambda ()
          (define d1 (car (list-ref some-dates 0)))
          (define d2 (car (list-ref some-dates 1)))
          (define d3 (car (list-ref some-dates 2)))
          (check-rt (pg-empty-range)
                    (pg-range d1 #t d3 #f)
                    (pg-range #f #f d2 #f)
                    (pg-range d3 #t #f #f))))
    (tc (mkconf 'tsrange)
        (lambda ()
          (match-define (list ts1 ts2 ts3)
            (map car some-timestamps/no-frac-sec))
          (check-rt (pg-empty-range)
                    (pg-range ts1 #t ts2 #t)
                    (pg-range ts1 #f ts3 #f)
                    (pg-range ts2 #f ts3 #t))))
    (tc (mkconf 'tstzrange #:check check-timestamptz-equal?)
        (lambda ()
          (match-define (list* ts1 ts2 ts3 _)
            (map car some-timestamptzs))
          (check-rt (pg-empty-range)
                    (pg-range ts1 #t ts2 #t)
                    (pg-range ts1 #f ts3 #f)
                    (pg-range ts2 #f ts3 #t)))))
  (void))

;; ------------------------------------------------------------

(define (test-mysql)

  (tc (mkconf 'signed-tinyint   #:param #f #:type "tinyint signed")    (tt-int 8))
  (tc (mkconf 'signed-smallint  #:param #f #:type "smallint signed")   (tt-int 16))
  (tc (mkconf 'signed-mediumint #:param #f #:type "mediumint signed")  (tt-int 24))
  (tc (mkconf 'signed-integer   #:param #f #:type "integer signed")    (tt-int 32))
  (tc (mkconf 'signed-bigint    #:cast "signed" #:type "bigint signed") (tt-int 64))

  (tc (mkconf 'unsigned-tinyint   #:param #f #:type "tinyint unsigned")    (tt-uint 8))
  (tc (mkconf 'unsigned-smallint  #:param #f #:type "smallint unsigned")   (tt-uint 16))
  (tc (mkconf 'unsigned-mediumint #:param #f #:type "mediumint unsigned")  (tt-uint 24))
  (tc (mkconf 'unsigned-integer   #:param #f #:type "integer unsigned")    (tt-uint 32))
  (tc (mkconf 'unsigned-bigint    #:cast "unsigned" #:type "bigint unsigned") (tt-uint 64))

  (tc (mkconf 'decimal #:type "decimal(30,10)") t-numeric/30/10)
  ;; NUMERIC is alias for DECIMAL

  (tc (mkconf 'real #:type "float") t-float4)
  (tc (mkconf 'double #:type "double precision") t-float8)

  ;; BIT ??
  ;; ENUM
  ;; SET

  ;; MySQL (<5.7) does not *store* fractional seconds
  (tc (mkconf 'date) t-date)
  (tc (mkconf 'time) t-time)
  (tc (mkconf 'datetime) t-timestamp)
  ;; TIMESTAMP -- like timestamptz, FIXME

  (tc (mkconf 'char #:type "char(10)") t-char/10)
  (tc (mkconf 'varchar  #:cast "char" #:type "varchar(4000)") (tt-string 4000 #f))
  (tc (mkconf 'text     #:cast "char") (tt-string #f 64000))
  (tc (mkconf 'longtext #:cast "char") (tt-string LONG #f)) ;; etc

  (tc (mkconf 'binary  #:cast "binary" #:type "binary(10)") t-bytes/10)
  (tc (mkconf 'varbinary #:cast "binary" #:type "varbinary(4000)") (tt-bytes 4000))
  (tc (mkconf 'blob      #:cast "binary") (tt-bytes 64000))
  (tc (mkconf 'longblob  #:cast "binary") (tt-bytes LONG)) ;; etc

  (unless (ORFLAGS 'ssl 'benchmark)
    (test-case "large blob"
      (call-with-connection
       (lambda (dbc)
         ;; Test able to read large blobs (depends on max_allowed_packet, though)
         (define max-allowed-packet (query-value dbc "select @@session.max_allowed_packet"))
         (for ([N (in-list '(#e1e6 #e1e7 #;#e1e8))] #:when (<= N max-allowed-packet))
           (define r (query-value dbc "select cast(repeat('a', ?) as binary)" N))
           (check-pred bytes? r)
           (check-equal? r (make-bytes N (char->integer #\a)))))
       #f)))

  (tc (mkconf 'vector #:param "string_to_vector(vector_to_string(?))")
      (lambda ()
        (check-rt (flvector 1.5)
                  (flvector 1.0 2.0 3.0))))

  ;; MySQL 8.0 and later: geometry types sent as blobs (WKB), but requires
  ;; explicit ST_GeomFromWKB conversion; cannot insert directly into field.
  (tc (mkconf 'geometry
              #:param "ST_GeomFromWKB(?)"
              #:insert-column "ST_GeomFromWKB(?)")
      t-geom-point
      t-geom-line-string
      t-geom-polygon)
  ;; POINT, LINESTRING, POLYGON
  ;; MULTI{POINT,LINESTRING,POLYGON}; GEOMETRYCOLLECTION

  (when (FLAG 'my:json)
    (tc (mkconf 'json #:type "json" #:wrap mysql-json) t-json))
  (void))

;; ------------------------------------------------------------

(define (test-sqlite3)
  (tc (mkconf 'integer) (tt-int 64))
  (tc (mkconf 'double)  t-float8)
  (tc (mkconf 'text)    (tt-string LONG #f))
  (tc (mkconf 'blob)    (tt-bytes LONG))
  (void))

;; ------------------------------------------------------------

(define (test-sql-server)
  (tc (mkconf 'tinyint)  (tt-uint 8)) ;; unsigned
  (tc (mkconf 'smallint) (tt-int 16)) ;; signed
  (tc (mkconf 'int)      (tt-int 32))
  (tc (mkconf 'bigint)   (tt-int 64))
  (tc (mkconf 'real)     t-float4)  ;; alias for FLOAT(24), single-precision
  (tc (mkconf 'float)    t-float8)  ;; defaults to FLOAT(53), double-precision

  (tc (mkconf 'decimal #:type "decimal(30,10)") t-numeric/30/10)
  ;; NUMERIC is alias for DECIMAL
  ;; MONEY, SMALLMONEY
  ;; BIT

  (tc (mkconf 'date) t-date)
  (tc (mkconf 'time) t-time)
  (tc (mkconf 'datetime)    t-timestamp)
  (tc (mkconf 'datetime2)   t-timestamp)    ;; expanded range and precision
  ;; SMALLDATETIME -- smaller range, no seconds
  ;; DATETIMEOFFSET -- datetime + tzoffset, FIXME?

  (tc (mkconf 'char
              #:param "cast(? collate LATIN1_GENERAL_100_CI_AS_SC_UTF8 as char(10))"
              #:type "char(10) collate LATIN1_GENERAL_100_CI_AS_SC_UTF8")
      t-char/10)
  (tc (mkconf 'varchar
              #:param "cast(? collate LATIN1_GENERAL_100_CI_AS_SC_UTF8 as varchar(8000))"
              #:type "varchar(8000) collate LATIN1_GENERAL_100_CI_AS_SC_UTF8")
      (tt-string #f 8000))
  (tc (mkconf 'varchar-max
              #:param "cast(? collate LATIN1_GENERAL_100_CI_AS_SC_UTF8 as varchar(max))"
              #:type "varchar(max) collate LATIN1_GENERAL_100_CI_AS_SC_UTF8")
      (tt-string LONG #f))

  (tc (mkconf 'binary    #:type "binary(10)") t-bytes/10)
  (tc (mkconf 'varbinary #:type "varbinary(max)") (tt-bytes LONG))

  (tc (mkconf 'text) ;; deprecated, does not support unicode collations
      (tt-string LONG #f #:cs 'ascii))
  (tc (mkconf 'ntext) ;; deprecated
      (tt-string LONG #f))
  ;; IMAGE -- deprecated, seems to be mapped to UNKNOWN

  ;; GEOGRAPHY
  ;; GEOMETRY
  ;; JSON
  ;; VECTOR
  ;; XML
  (void))

;; ------------------------------------------------------------

(define (test-oracle)
  ;; Note: varchar test fails on Oracle if NLS_LANG not set (see config, test-dsn.rktd)
  ;; It appears that for a VARCHAR2 column, data is translated as follows:
  ;;   [UTF16]      --> [ODBC internal charset] --> [column charset]
  ;;   SQL_WVARCHAR     from NLS_LANG               from column or db default
  ;; There does not appear to be a way to configure the intermediate charset other
  ;; than setting the NLS_LANG environment variable. Does not affect NVARCHAR2.
  ;; - https://stackoverflow.com/questions/1998771/in-oracle-how-do-i-change-my-session-to-display-utf8
  ;; - https://docs.oracle.com/en/database/oracle/oracle-database/26/nlspg/setting-up-globalization-support-environment.html
  ;; - https://docs.oracle.com/en/database/oracle/oracle-database/26/odbcd/unicode-support.html
  ;; - https://www.oracle.com/database/technologies/faq-nls-lang.html
  (unless (regexp-match? #rx"[.](?:AL32)?UTF8$" (or (getenv "NLS_LANG") ""))
    (eprintf "warning [oracle tests]: envvar NLS_LANG not set to a UTF8 charset;\n")
    (eprintf " expect VARCHAR tests to fail\n"))

  (tc (mkconf 'varchar2  #:type "varchar2(255 CHAR)")  (tt-string 255 #f))
  (tc (mkconf 'nvarchar2 #:type "nvarchar2(255)") (tt-string 255 #f))
  ;; LONG deprecated

  ;; ODBC maps NUMBER to FLOAT
  (tc (mkconf 'number #:check check-ora-number-equal?) t-numeric-unbounded)
  (tc (mkconf 'binary_float)  t-float4)
  (tc (mkconf 'binary_double) t-float8)

  (tc (mkconf 'date) t-timestamp)
  (tc (mkconf 'timestamp) t-timestamp)
  ;; (tc (mkconf 'timestamp+tz #:type "timestamp with time zone") __) ;; FIXME
  (tc (mkconf 'timestamptz #:type "timestamp with local time zone") t-timestamptz)
  ;; INTERVAL ...

  (tc (mkconf 'raw     #:param #f #:type "raw(2000)") (tt-bytes 2000))
  (tc (mkconf 'longraw #:param #f #:type "long raw")  (tt-bytes LONG))

  (tc (mkconf 'char   #:type "char(10)")  t-char/10)
  (tc (mkconf 'nchar  #:type "nchar(10)") t-nchar/10)
  (tc (mkconf 'clob   #:param #f) (tt-string LONG #f))
  (tc (mkconf 'nclob  #:param #f) (tt-string LONG #f))
  (tc (mkconf 'blob   #:param #f) (tt-bytes LONG))
  (void))

(define-check (check-ora-number-equal? actual expected)
  (check-= actual expected 0.001))

;; ------------------------------------------------------------

(define (test-db2)
  (tc (mkconf 'smallint) (tt-int 16))
  (tc (mkconf 'integer)  (tt-int 32))
  (tc (mkconf 'bigint)   (tt-int 64))
  (tc (mkconf 'real)     t-float4)
  (tc (mkconf 'double)   t-float8)
  (tc (mkconf 'decimal #:type "decimal(30,10)") t-numeric/30/10)
  ;; NUMERIC is alias for decimal (?)
  ;; DECFLOAT

  (tc (mkconf 'char    #:type "char(10)")       t-char/10)
  (tc (mkconf 'varchar #:type "varchar(255)")   (tt-string 255 #f))
  (tc (mkconf 'graphic #:type "graphic(10)")    t-nchar/10)
  (tc (mkconf 'vargraphic #:type "vargraphic(2000)")  (tt-string 2000 #f))
  (tc (mkconf 'clob)    (tt-string LONG #f))
  (tc (mkconf 'dbclob)  (tt-string LONG #f))

  (tc (mkconf 'binary #:type "binary(10)") t-bytes/10)
  (tc (mkconf 'varbinary #:type "varbinary(2000)") (tt-bytes 2000))
  (tc (mkconf 'blob) (tt-bytes LONG))

  (tc (mkconf 'date) t-date)
  (tc (mkconf 'time) t-time)
  (tc (mkconf 'timestamp) t-timestamp)

  ;; BOOLEAN
  ;; XML
  ;; arrays (?)
  ;; VECTOR
  (void))

;; ------------------------------------------------------------

;; The Firebird ODBC driver maps Firebird SQL result types to ODBC types in a
;; way that makes automatic type conversions (as the db library attempts to do)
;; difficult. Specifically, with Firebird server v4, ODBC driver v3.0.0:
;; - INT128     to SQL_WVARCHAR (tests fail)
;; - DECIMAL    to SQL_WVARCHAR (tests fail)
;; - VARCHAR    to SQL_WVARCHAR
;; - VARBINARY  to SQL_VARCHAR  (tests fail)

(define (test-firebird)
  (tc (mkconf 'smallint) (tt-int 16))
  (tc (mkconf 'integer)  (tt-int 32))
  (tc (mkconf 'bigint)   (tt-int 64))
  (tc (mkconf 'int128)   (tt-int 128))
  (tc (mkconf 'real)     t-float4)
  (tc (mkconf 'double  #:type "double precision") t-float8)
  (tc (mkconf 'decimal #:type "decimal(30,10)") t-numeric/30/10)
  ;; NUMERIC is alias for DECIMAL
  ;; DECFLOAT

  (tc (mkconf 'date) t-date)
  (tc (mkconf 'time) t-time)
  (tc (mkconf 'timestamp) t-timestamp) ;; alias for TIMESTAMP WITHOUT TIME ZONE
  ;; TIMESTAMP WITH TIME ZONE

  (tc (mkconf 'char      #:type "char(10)")         t-char/10)
  (tc (mkconf 'varchar   #:type "varchar(2000)")    (tt-string 2000 #f))
  (tc (mkconf 'binary    #:type "binary(10)")       t-bytes/10)
  (tc (mkconf 'varbinary #:type "varbinary(2000)")  (tt-bytes 2000))
  ;; NCHAR alias for CHAR CHARACTER SET ISO8859_1 (aka Latin1)

  (tc (mkconf 'boolean) t-boolean)
  (tc (mkconf 'blob) (tt-bytes LONG))
  ;; arrays (?)
  (void))

;; ------------------------------------------------------------

(define (test-access)
  ;; TINYINT
  (tc (mkconf 'short  #:param "CInt(?)") (tt-int 16))
  (tc (mkconf 'long   #:param "CLng(?)")  (tt-int 32))
  (tc (mkconf 'single #:param "CSng(?)")  t-float4)
  (tc (mkconf 'double #:param "CDbl(?)")  t-float8)
  ;; Apparently, Access only allows creation of DECIMAL fields in some
  ;; combinations of drivers and file properties, and my testing setup does not
  ;; satisy its requirements. Attempting to create the rt table with a DECIMAL
  ;; field gives a syntax error.
  (tc (mkconf 'decimal #:param "CDec(?)" #:type #f #;"decimal(30,10)") t-numeric/30/10)
  ;; MONEY / CURRENCY
  ;; UNIQUEIDENTIFIER / GUID
  (tc (mkconf 'datetime #:param "CDate(?)") t-timestamp)
  (tc (mkconf 'varchar #:param "CStr(?)" #:type "varchar(255)") (tt-string 255 #f))
  (tc (mkconf 'longtext #:param #f) (tt-string 2000 #f)) ;; aka MEMO
  (tc (mkconf 'varbinary #:param #f #:type "varbinary(255)") (tt-bytes 255))
  (tc (mkconf 'longbinary #:param #f) (tt-bytes 2000))
  (void))

;; ============================================================

(define test
  (test-suite "SQL types (roundtrip, etc)"
    (cond [(FLAG 'postgresql) (test-postgresql)]
          [(FLAG 'mysql) (test-mysql)]
          [(FLAG 'sqlite3) (test-sqlite3)]
          [(FLAG 'ismss) (test-sql-server)]
          [(FLAG 'isora) (test-oracle)]
          [(FLAG 'isdb2) (test-db2)]
          [(FLAG 'isfbd) (test-firebird)]
          [(FLAG 'isacc) (test-access)]
          [else (test-case "unknown driver"
                  (fail-check "unknown driver"))])))

;; ============================================================

#;
(define-check (check-varchar str)
  ;; Check roundtrip (only checks same when arrives back at client)
  (check-roundtrip str)
  ;; Also must check correct on server side, so...
  ;;  - check the string has the right length
  (define param-len-sql
    (cond [(ORFLAGS 'postgresql 'ispg 'sqlite3) "length($1)"]
          [(ORFLAGS 'isora) "cast(length(?) as integer)"]
          [(ORFLAGS 'mysql 'ismy 'isdb2) "char_length(?)"]
          [(ORFLAGS 'ismss)
           (case (current-type)
             [(varchar) "len(cast(? as varchar(255)))"]
             [(nvarchar) "len(cast(? as nvarchar(255)))"]
             [else #f])]
          [else #f]))
  (when (string? param-len-sql)
    (check-equal? (query-value (dbc) (select-val param-len-sql) str)
                  (string-length str)
                  "check server-side length")))
