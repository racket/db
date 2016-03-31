#lang racket/unit
(require rackunit
         racket/class
         racket/list
         racket/math
         racket/match
         racket/string
         (prefix-in srfi: srfi/19)
         db/base
         db/util/datetime
         db/util/geometry
         db/util/postgresql
         "../config.rkt")
(import config^ database^)
(export test^)

(define current-type (make-parameter #f))
(define dbc (make-parameter 'db-not-set))

(define-syntax-rule (type-test-case types . body)
  (type-test-case* types (lambda () . body)))

(define (type-test-case* types proc)
  (let* ([known-types
          (if (ORFLAGS 'sqlite3)
              '(bigint double text blob)
              (send dbsystem get-known-types +inf.0))]
         [type (for/or ([type types])
                 (and (member type known-types) type))])
    (when type
      (test-case (format "~s" type)
        (call-with-connection
         (lambda (c)
           (parameterize ((current-type type)
                          (dbc c))
             (proc))))))))

;; ----------------------------------------

(define-check (check-roundtrip value)
  (check-roundtrip* value check-equal?))

(define (check-roundtrip* value check-equal?)
  (cond [(roundtrip-statement)
         => (lambda (rt-stmt)
              (check-equal? (query-value (dbc) rt-stmt value) value))]
        [else
         (error 'check-roundtrip "don't know how to check-roundtrip type: ~e"
                (current-type))]))

(define (roundtrip-statement)
  (cond [(ORFLAGS 'postgresql 'ispg)
         (format "select $1::~a" (type->sql (current-type)))]
        [(ORFLAGS 'mysql 'ismy)
         (roundtrip-stmt/mysql)]
        [(eq? dbsys 'sqlite3) ;; no ODBC-sqlite3, too painful
         "select ?"]
        [(FLAG 'isora)
         (roundtrip-stmt/oracle)]
        [(FLAG 'isdb2)
         (roundtrip-stmt/db2)]
        [else #f]))

(define (roundtrip-stmt/mysql)
  (case (current-type)
    [(varchar text) "select ?"] ;;  "select cast(? as char)" gives ODBC problems
    [(blob) "select cast(? as binary)"] ;; ???
    [(integer tinyint smallint mediumint bigint)
     "select cast(? as signed integer)"]
    [(double real) "select (? * 1.0)"]
    [(decimal numeric) "select cast(? as decimal(40,10))"]
    [(date) "select cast(? as date)"]
    [(time) "select cast(? as time)"]
    [(datetime) "select cast(? as datetime)"]
    [(json geometry) "select ?"]
    ;; FIXME: more types
    [else #f]))

(define (roundtrip-stmt/oracle)
  (define (wrap e) (format "select ~a from dual" e))
  (case (current-type)
    [(varchar)  (wrap "cast(? as varchar2(200))")]
    [(blob)     (wrap "cast(? as binary)")] ;; ??
    [(integer)  (wrap "cast(? as integer)")]
    ;; FIXME: real
    [(numeric)  (wrap "cast(? as decimal(20,10))")]
    ;; FIXME: date
    ;; FIXME: time (bug?)
    [(datetime) (wrap "cast(? as datetime)")]
    ;; FIXME: more types
    [else #f]))

(define (roundtrip-stmt/db2)
  (define (wrap e) (format "select ~a" e))
  (case (current-type)
    [(varchar)  (wrap "cast(? as varchar(200) ccsid unicode)")]
    [(blob)     (wrap "cast(? as binary)")] ;; ??
    [(integer)  (wrap "cast(? as integer)")]
    ;; FIXME: real
    [(numeric)  (wrap "cast(? as decimal)")]
    [(date)     (wrap "cast(? as date)")]
    [(time)     (wrap "cast(? as time)")]
    [(datetime) (wrap "cast(? as datetime)")]
    ;; FIXME: more types
    [else #f]))

(define (type->sql type)
  (cond [(ORFLAGS 'postgresql 'ispg)
         (cond [(eq? type 'double) 'float8]
               [(eq? type 'char1)  '|"char"|]
               [(regexp-match #rx"^(.*)-array$" (format "~a" type))
                => (lambda (m) (format "~a[]" (type->sql (string->symbol (cadr m)))))]
               [else type])]
        [else type]))

;; ----------------------------------------

(define (setup-temp-table [type (current-type)])
  (define (temp-table-ok?) (ORFLAGS 'postgresql 'mysql))
  (and (temp-table-ok?)
       (query-exec (dbc)
         (format "create temporary table testing_temp_table (v ~a)" (type->sql type)))
       #t))

(define-check (check-table-rt value)
  (check-table-rt* value check-equal?))

(define (check-table-rt* value check-equal?)
  (query-exec (dbc) "delete from testing_temp_table")
  (query-exec (dbc) (sql "insert into testing_temp_table (v) values ($1)") value)
  (check-equal? (query-value (dbc) "select v from testing_temp_table") value))

;; ----------------------------------------

(define-check (check-value/text val text)
  (check-value/text* val text check-equal? check-equal?))

(define (check-value/text* val text check-val-equal? check-text-equal?)
  (cond [(ORFLAGS 'postgresql)
         (let* ([tname (type->sql (current-type))]
                [q-text->val (sql (format "select ($1::text)::~a" tname))]
                [q-val->text (sql (format "select ($1::~a)::text" tname))])
           (when check-val-equal?
             (check-val-equal? (query-value (dbc) q-text->val text) val))
           (when check-text-equal?
             (check-text-equal? (query-value (dbc) q-val->text val) text)))]
        ;; FIXME: mysql just test val->text since text->val irregular
        [else (void)]))

(define-check (check-varchar  str)
  ;; Check roundtrip (only checks same when arrives back at client)
  (check-roundtrip str)
  ;; Also must check correct on server side, so...
  ;;  - check the string has the right length
  (let ([len-fun (case dbsys
                   ((postgresql sqlite3) "length")
                   ((mysql) "char_length")
                   ((odbc) (cond [(ANDFLAGS 'ispg) "length"]
                                 [(ANDFLAGS 'ismy) "char_length"])))])
    (when (string? len-fun)
      (check-equal? (query-value (dbc) (sql (format "select ~a($1)" len-fun)) str)
                    (string-length str)
                    "check server-side length"))))

(define-check (check-1char str)
  (check-varchar str)
  (when (= (string-length str) 1)
    ;;  - if one char, check server-side char->int
    (let ([ci-fun (case dbsys
                    ((postgresql) "ascii") ;; yes, returns unicode code point too (if utf8)
                    ((mysql sqlite3) #f) ;; ???
                    ((odbc) (cond [(ANDFLAGS 'ispg) "ascii"])))])
      (when (string? ci-fun)
        (check-equal? (query-value (dbc) (sql (format "select ~a($1)" ci-fun)) str)
                      (char->integer (string-ref str 0))
                      "check server-side char->int")))
    ;;  - if one char, check server-side int->char
    (let ([ic-fmt (case dbsys
                    ((postgresql) "select chr(~a)")
                    ((mysql) "select char(~a using utf8)")
                    ((sqlite3) #f)
                    ((odbc) (cond [(ANDFLAGS 'ispg) "select chr(~a)"]
                                  [(ANDFLAGS 'ismy) "select char(~a using utf8)"])))])
      (when (string? ic-fmt)
        (check-equal? (query-value (dbc)
                        (format ic-fmt
                                (if (ORFLAGS 'mysql 'ismy)
                                    (string-join
                                     (map number->string
                                          (bytes->list (string->bytes/utf-8 str)))
                                     ", ")
                                    (char->integer (string-ref str 0)))))
                      str
                      "check server-side int->char")))))

(define-check (check-=any elt lst in?)
  (check-equal? (query-value (dbc) (format "select ~a = any ($1)" elt) (list->pg-array lst))
                in?))

(define-check (check-trim-string=? a b)
  (check-equal? (string-trim a) (string-trim b)))

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

;; ----------------------------------------

(define some-dates
  `((,(sql-date 1776 07 04) "1776-07-04")
    (,(sql-date 2000 01 01) "2000-01-01")
    (,(sql-date 2012 02 14) "2012-02-14")))

(define some-times
  `((,(sql-time 01 02 03 0 #f) "01:02:03")
    (,(sql-time 12 34 56 0 #f) "12:34:56")
    (,(sql-time 17 30 01 0 #f) "17:30:01")
    (,(sql-time 01 02 03 #e4e7 #f) "01:02:03.04")
    (,(sql-time 12 34 56 123456000 #f) "12:34:56.123456")))

(define some-timetzs
  `((,(sql-time 01 02 03 0 3600) "01:02:03+01")
    (,(sql-time 12 34 56 0 3600) "12:34:56+01")
    (,(sql-time 17 30 01 0 -7200) "17:30:01-02")
    (,(sql-time 01 02 03 #e4e7 3600) "01:02:03.04+01")
    (,(sql-time 12 34 56 123456000 3600) "12:34:56.123456+01")
    (,(sql-time 12 34 56 123456000 -7200) "12:34:56.123456-02")))

(define some-timestamps
  `((,(sql-timestamp 2000 01 01 12 34 56 0 #f) "2000-01-01 12:34:56")
    (,(sql-timestamp 1776 07 04 12 34 56 0 #f) "1776-07-04 12:34:56")
    (,(sql-timestamp 2012 02 14 12 34 56 0 #f) "2012-02-14 12:34:56")
    (,(sql-timestamp 2000 01 01 12 34 56 123456000 #f) "2000-01-01 12:34:56.123456")
    (,(sql-timestamp 1776 07 04 12 34 56 123456000 #f) "1776-07-04 12:34:56.123456")
    (,(sql-timestamp 2012 02 14 12 34 56 123456000 #f) "2012-02-14 12:34:56.123456")))
(define some-pg-timestamps
  `((-inf.0 "-infinity")
    (+inf.0 "infinity")))

(define some-timestamptzs
  `((,(sql-timestamp 2000 01 01 12 34 56 0 -14400) "2000-01-01 12:34:56-04")
    (,(sql-timestamp 1776 07 04 12 34 56 0 -14400) "1776-07-04 12:34:56-04")
    (,(sql-timestamp 2012 02 14 12 34 56 0 7200) "2012-02-14 12:34:56+02")
    (,(sql-timestamp 2000 01 01 12 34 56 123456000 14400) "2000-01-01 12:34:56.123456+04")
    (,(sql-timestamp 1776 07 04 12 34 56 123456000 -14400) "1776-07-04 12:34:56.123456-04")
    (,(sql-timestamp 2012 02 14 12 34 56 123456000 -7200) "2012-02-14 12:34:56.123456-02")
    (-inf.0 "-infinity")
    (+inf.0 "infinity")))

(define some-intervals
  `((,(sql-interval 0 0 3 4 5 6 0) "3 days 04:05:06")
    (,(sql-interval 87 1 0 0 0 0 0) "87 years 1 mon")
    (,(sql-interval 1 2 3 4 5 6 45000) "1 year 2 mons 3 days 04:05:06.000045")
    (,(sql-interval 0 0 -3 -4 -5 -6 0) "-3 days -04:05:06")
    (,(sql-interval -87 -1 0 0 0 0 0) "-87 years -1 mons")
    (,(sql-interval -1 -2 3 4 5 6 45000) "-1 years -2 mons +3 days 04:05:06.000045")))

(define some-basic-strings
  `("Az0"
    "this is the time to remember"
    "it's like that (and that's the way it is)"
    ,(string #\\)
    ,(string #\\ #\\)
    ,(string #\')
    ,(string #\\ #\')
    "λ the ultimate"))
(define some-intl-strings
  `("αβψδεφγηιξκλμνοπρστθωςχυζ"
    "अब्च्देघिज्क्ल्म्नोप्र्स्तुव्य्"
    "شﻻؤيثبلاهتنمةىخحضقسفعرصءغئ"
    "阿あでいおうわぁ"
    "абцдефгхиклмнопљрстувњџзѕЋч"))

;; ============================================================

(define test
  (test-suite "SQL types (roundtrip, etc)"

    (type-test-case '(bool boolean)
      (check-roundtrip #t)
      (check-roundtrip #f)
      (when (setup-temp-table)
        (check-table-rt #t)
        (check-table-rt #f)))

    (type-test-case '(bytea blob)
      (check-roundtrip #"this is the time to remember")
      (check-roundtrip #"that's the way it is")
      (check-roundtrip (list->bytes (build-list 256 values)))
      (when (ORFLAGS 'postgresql 'mysql 'sqlite3)
        (check-roundtrip (make-bytes #e1e6 (char->integer #\a)))
        (check-roundtrip (make-bytes #e1e7 (char->integer #\b)))
        #| (check-roundtrip (make-bytes #e1e8 (char->integer #\c))) |#)
      (when (ORFLAGS 'postgresql)
        (let ([r (query-value (dbc) "select cast(repeat('a', 10000000) as bytea)")])
          (check-pred bytes? r)
          (check-equal? r (make-bytes 10000000 (char->integer #\a))))
        (let ([r (query-value (dbc) "select cast(repeat('a', 100000000) as bytea)")])
          (check-pred bytes? r)
          (check-equal? r (make-bytes 100000000 (char->integer #\a)))))
      (when (ORFLAGS 'mysql)
        ;; Test able to read large blobs
        ;; (depends on max_allowed_packet, though)
        (define max-allowed-packet (query-value (dbc) "select @@session.max_allowed_packet"))
        (for ([N (in-list '(#e1e7 #e1e8))])
          (when (<= N max-allowed-packet)
            (let* ([q (format "select cast(repeat('a', ~s) as binary)" N)]
                   [r (query-value (dbc) q)])
              (check-pred bytes? r)
              (check-equal? r (make-bytes N (char->integer #\a)))))))
      (when (setup-temp-table)
        (check-table-rt #"this is the time to remember")
        (check-table-rt #"I am the walrus.")))

    (type-test-case '(text)
      (check-roundtrip "")
      (check-roundtrip "abcde")
      (check-roundtrip (make-string #e1e6 #\a))
      (check-roundtrip (make-string #e1e7 #\b))
      (check-roundtrip (make-string #e1e8 #\c))
      (when (setup-temp-table)
        (check-table-rt "")
        (check-table-rt "abcde")))

    (type-test-case '(tinyint)
      (check-roundtrip 5)
      (check-roundtrip -1)
      (check-roundtrip 127)
      (check-roundtrip -128)
      (when (setup-temp-table)
        (check-table-rt 5)
        (check-table-rt -1)
        (check-table-rt 127)
        (check-table-rt -128)))

    (type-test-case '(smallint)
      (check-roundtrip 5)
      (check-roundtrip -1)
      (check-roundtrip #x7FFF)
      (check-roundtrip #x-8000)
      (when (setup-temp-table)
        (check-table-rt 1234)))

    (type-test-case '(integer)
      (check-roundtrip 5)
      (check-roundtrip -1)
      (check-roundtrip #x7FFFFFFF)
      (check-roundtrip #x-80000000)
      (when (setup-temp-table)
        (check-table-rt 123456)))

    (type-test-case '(bigint)
      (check-roundtrip 5)
      (check-roundtrip -1)
      (check-roundtrip (sub1 (expt 2 63)))
      (check-roundtrip (- (expt 2 63)))
      (when (setup-temp-table)
        (check-table-rt 123456)))

    (type-test-case '(mediumint)
      (check-roundtrip 5)
      (check-roundtrip -1)
      (check-roundtrip 1234)
      (when (setup-temp-table)
        (check-table-rt 5)
        (check-table-rt -1)
        (check-table-rt 123456)))

    (type-test-case '(real)
      (check-roundtrip 1.0)
      (check-roundtrip 1.5)
      (check-roundtrip -5.5)
      (when (send dbsystem has-support? 'real-infinities)
        (check-roundtrip +inf.0)
        (check-roundtrip -inf.0)
        (check-roundtrip +nan.0))
      (when (setup-temp-table)
        (check-table-rt 1.0)
        (check-table-rt -5.5)))

    (type-test-case '(double)
      (check-roundtrip 1.0)
      (check-roundtrip 1.5)
      (check-roundtrip -5.5)
      (check-roundtrip 1.1)
      (check-roundtrip -5.8)
      (when (send dbsystem has-support? 'real-infinities)
        (check-roundtrip +inf.0)
        (check-roundtrip -inf.0)
        (check-roundtrip +nan.0))
      (when (setup-temp-table (if (ORFLAGS 'pg 'ispg) "float8" "double"))
        (check-table-rt 1.0)
        (check-table-rt -5.5)))

    (unless (ORFLAGS 'isdb2) ;; "Driver not capable"
      (type-test-case '(numeric decimal)
        (check-roundtrip 0)
        (check-roundtrip 10)
        (check-roundtrip -5)
        (unless (ANDFLAGS 'odbc)
          (check-roundtrip 1234567890)
          (check-roundtrip -1234567890)
          (check-roundtrip #e12345.67809)
          (check-roundtrip #e-12345.67809)
          (unless (ANDFLAGS 'mysql)
            (check-roundtrip 12345678901234567890)
            (check-roundtrip -12345678901234567890)
            (check-roundtrip #e1234567890.0987654321)
            (check-roundtrip #e-1234567890.0987654321))
          (check-roundtrip 1/2)
          (check-roundtrip 1/40)
          (check-roundtrip 1/10)
          (check-roundtrip -1/2)
          (check-roundtrip -1/40)
          (check-roundtrip -1/10)
          (check-roundtrip 1/400000))
        (when (send dbsystem has-support? 'numeric-infinities)
          (check-roundtrip +nan.0))
        (when (setup-temp-table (if (ORFLAGS 'mysql 'ismy) "decimal(40,10)" (current-type)))
          (check-table-rt 1234567890)
          (check-table-rt 1/2))))

    (type-test-case '(varchar)
      (unless (ORFLAGS 'isora) ;; Oracle treats empty string as NULL (?!)
        (check-varchar ""))
      (for ([str some-basic-strings])
        (check-varchar str))
      (for ([str some-intl-strings])
        (check-varchar str)
        ;; and do the extra one-char checks:
        (check-1char (substring str 0 1)))
      (unless (ORFLAGS 'isora 'isdb2)
        (check-varchar (make-string 800 #\a)))
      (unless (ORFLAGS 'isora 'isdb2) ;; too long
        (check-varchar (apply string-append some-intl-strings)))
      ;; one-char checks
      (check-1char (string #\λ))
      (check-1char (make-string 1 #\u2200))
      (check-varchar (make-string 20 #\u2200))
      ;; check large strings
      (unless (ORFLAGS 'isora 'isdb2) ;; too long (???)
        (check-varchar (make-string 100 #\u2200)))
      ;; Following might not produce valid string (??)
      (unless (ORFLAGS 'isora 'isdb2)
        (check-varchar (list->string
                        (build-list 800
                                    (lambda (n)
                                      (integer->char (add1 n)))))))
      (unless (ORFLAGS 'mysql 'ismy)
        (when (setup-temp-table)
          (for ([str (append some-basic-strings some-intl-strings)])
            (check-table-rt str)))))

    (type-test-case '(character)
      (when (setup-temp-table "char(5)")
        (check-table-rt* "" check-trim-string=?)
        (check-table-rt* "abc" check-trim-string=?)
        (check-table-rt* "abcde" check-trim-string=?)))

    (type-test-case '(date)
      (for ([d+s some-dates])
        (check-roundtrip (car d+s))
        (check-value/text (car d+s) (cadr d+s)))
      (when (setup-temp-table)
        (for ([d+s some-dates])
          (check-table-rt (car d+s)))))

    (type-test-case '(time)
      (define frac-seconds-ok? (ORFLAGS 'postgresql))
      (for ([t+s some-times])
        (when (or frac-seconds-ok? (zero? (sql-time-nanosecond (car t+s))))
          (check-roundtrip (car t+s))
          (check-value/text (car t+s) (cadr t+s))))
      (when (setup-temp-table)
        (for ([t+s some-times])
          ;; MySQL (<5.7) does not *store* fractional seconds
          (when (or frac-seconds-ok? (zero? (sql-time-nanosecond (car t+s))))
            (check-table-rt (car t+s))))))

    (type-test-case '(timetz)
      (for ([t+s some-timetzs])
        (check-roundtrip (car t+s))
        (check-value/text (car t+s) (cadr t+s)))
      (when (setup-temp-table)
        (for ([t+s some-timetzs])
          (check-table-rt (car t+s)))))

    (type-test-case '(timestamp datetime)
      (define frac-seconds-ok? (ORFLAGS 'postgresql))
      (for ([t+s some-timestamps])
        (when (or frac-seconds-ok? (zero? (sql-timestamp-nanosecond (car t+s))))
          (check-roundtrip (car t+s))
          (check-value/text (car t+s) (cadr t+s))))
      (when (ORFLAGS 'postgresql) ;; Only postgresql supports +/-inf.0
        (for ([t+s some-pg-timestamps])
          (check-roundtrip (car t+s))
          (check-value/text (car t+s) (cadr t+s))))
      (when (setup-temp-table)
        (for ([t+s some-timestamps])
          ;; MySQL (<5.7) does not *store* fractional seconds
          (when (or frac-seconds-ok? (zero? (sql-timestamp-nanosecond (car t+s))))
            (check-table-rt (car t+s))))))

    (type-test-case '(timestamptz)
      (for ([t+s some-timestamptzs])
        (check-roundtrip* (car t+s) check-timestamptz-equal?)
        (check-value/text* (car t+s) (cadr t+s) check-timestamptz-equal? #f))
      (when (setup-temp-table)
        (for ([t+s some-timestamptzs])
          (check-table-rt* (car t+s) check-timestamptz-equal?))))

    (type-test-case '(interval)
      (for ([i+s some-intervals])
        (when (or (memq dbsys '(postgresql))
                  (sql-day-time-interval? i+s)
                  (sql-year-month-interval? i+s))
          (check-roundtrip (car i+s))
          (when (memq dbsys '(postgresql))
            (check-value/text (car i+s) (cadr i+s))))))

    (unless (ORFLAGS 'mysql)
      (type-test-case '(varbit bit)
        (for ([s (list "1011"
                       "000000"
                       (make-string 30 #\1)
                       (string-append (make-string 10 #\1) (make-string 20 #\0)))])
          (check-roundtrip* (string->sql-bits s) check-bits-equal?))))

    (type-test-case '(point geometry)
      (check-roundtrip (point 0 0))
      (check-roundtrip (point 1 2))
      (check-roundtrip (point (exp 1) pi)))

    (type-test-case '(line-string geometry)
      (check-roundtrip (line-string (list (point 0 0) (point 1 1))))
      (check-roundtrip (line-string (list (point 0 0) (point 1 1) (point -5 7)))))
    
    (type-test-case '(lseg geometry)
      (check-roundtrip (line-string (list (point 0 0) (point 1 1)))))

    (type-test-case '(polygon geometry)
      (check-roundtrip
       (polygon (line-string (list (point 0 0) (point 2 0) (point 1 1) (point 0 0))) '()))
      (when (memq dbsys '(mysql))
        (check-roundtrip
         (polygon (line-string (list (point 0 0) (point 4 0) (point 2 2) (point 0 0)))
                  (list (line-string (list (point 1 1) (point 3 1)
                                           (point 1.5 1.5) (point 1 1))))))))

    (when (ORFLAGS 'postgresql) ;; "Driver not capable"
      (type-test-case '(path)
        (check-roundtrip (pg-path #t (list (point 0 0) (point 1 1) (point -5 7))))
        (check-roundtrip (pg-path #f (list (point -1 -1) (point (exp 1) pi)))))
      (type-test-case '(box)
        (check-roundtrip (pg-box (point 10 10) (point 2 8))))
      (type-test-case '(circle)
        (check-roundtrip (pg-circle (point 1 2) 45))))

    (when (ANDFLAGS 'postgresql 'pg92)
      (type-test-case '(json)
        (define some-jsexprs
          (list #t #f 0 1 -2 pi "" "hello" "good\nbye" 'null
                (hasheq 'a 1 'b 2 'c 'null)
                (list #t #f 'null "a" "b")))
        (for ([j some-jsexprs])
          (check-roundtrip j)))
      (type-test-case '(int4range)
        (check-roundtrip (pg-empty-range))
        ;; for now, only test things in canonical form... (FIXME)
        (check-roundtrip (pg-range 0 #t 5 #f))
        (check-roundtrip (pg-range #f #f -57 #f))
        (check-roundtrip (pg-range 1234 #t #f #f)))
      (type-test-case '(int8range)
        (check-roundtrip (pg-empty-range))
        ;; for now, only test things in canonical form... (FIXME)
        (check-roundtrip (pg-range 0 #t 5 #f))
        (check-roundtrip (pg-range #f #f -57 #f))
        (check-roundtrip (pg-range 1234 #t #f #f))
        (check-roundtrip (pg-range (expt 2 60) #t (expt 2 61) #f)))
      ;; FIXME: numrange
      (type-test-case '(daterange)
        (define d1 (car (first some-dates)))
        (define d2 (car (second some-dates)))
        (define d3 (car (third some-dates)))
        (check-roundtrip (pg-empty-range))
        ;; for now, only test things in canonical form... (FIXME?)
        (check-roundtrip (pg-range d1 #t d3 #f))
        (check-roundtrip (pg-range #f #f d2 #f))
        (check-roundtrip (pg-range d3 #t #f #f)))
      (type-test-case '(tsrange)
        (define ts1 (car (second some-timestamps)))
        (define ts2 (car (first some-timestamps)))
        (define ts3 (car (third some-timestamps)))
        (check-roundtrip (pg-empty-range))
        (check-roundtrip (pg-range ts1 #t ts2 #t))
        (check-roundtrip (pg-range ts1 #f ts3 #f))
        (check-roundtrip (pg-range ts2 #f ts3 #t)))
      (type-test-case '(tstzrange)
        (define ts1 (car (second some-timestamptzs)))
        (define ts2 (car (first some-timestamptzs)))
        (define ts3 (car (third some-timestamptzs)))
        (check-roundtrip (pg-empty-range))
        (check-roundtrip* (pg-range ts1 #t ts2 #t) check-timestamptz-equal?)
        (check-roundtrip* (pg-range ts1 #f ts3 #f) check-timestamptz-equal?)
        (check-roundtrip* (pg-range ts2 #f ts3 #t) check-timestamptz-equal?)))

    ;; --- Arrays ---
    (type-test-case '(boolean-array)
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list #t)))
      (check-roundtrip (list->pg-array (list #t #t #f)))
      (check-roundtrip (list->pg-array (list #t sql-null #f)))
      (check-=any "'t'::boolean" (list) #f)
      (check-=any "'t'::boolean" (list #f) #f)
      (check-=any "'t'::boolean" (list sql-null #t #f) #t))

    (type-test-case '(bytea-array)
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list #"abc")))
      (check-roundtrip (list->pg-array (list #"a" #"bc" #"def")))
      (check-roundtrip (list->pg-array (list #"a" sql-null #"bc")))
      (check-=any "'abc'::bytea" (list) #f)
      (check-=any "'abc'::bytea" (list #"a" #"abc") #t)
      (check-=any "'abc'::bytea" (list #"a" sql-null #"abc") #t))

    (type-test-case '(char1-array)
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list #\a)))
      (check-roundtrip (list->pg-array (list #\a #\z)))
      (check-roundtrip (list->pg-array (list #\a sql-null #\z)))
      (check-=any "'a'::\"char\"" (list) #f)
      (check-=any "'a'::\"char\"" (list #\a #\b) #t)
      (check-=any "'a'::\"char\"" (list #\a sql-null #\z) #t))

    (type-test-case '(smallint-array)
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list 1)))
      (check-roundtrip (list->pg-array (list 1 2 3)))
      (check-roundtrip (list->pg-array (list 1 sql-null 3 4 5)))
      (check-=any "1::smallint" (list) #f)
      (check-=any "1::smallint" (list 1 2 3) #t)
      (check-=any "1::smallint" (list sql-null 1 2 3) #t))

    (type-test-case '(integer-array)
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list 1)))
      (check-roundtrip (list->pg-array (list 1 2 3)))
      (check-roundtrip (list->pg-array (list 1 sql-null 3 4 5)))
      (check-=any "1" (list) #f)
      (check-=any "1" (list 1 2 3) #t)
      (check-=any "1" (list sql-null 1 2 3) #t))

    (type-test-case '(text-array)
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list "abc")))
      (check-roundtrip (list->pg-array (list "a" "" "bc" "def" "")))
      (check-roundtrip (list->pg-array (list "a" sql-null "")))
      (check-=any "'abc'" (list) #f)
      (check-=any "'abc'" (list "abc" "def") #t)
      (check-=any "'abc'" (list "abc" "def" sql-null) #t))

    #|
    (type-test-case '(character-array)
       ;; NOTE: seems to infer an elt type of character(1)...
       (check-roundtrip (list->pg-array (list)))
       (check-roundtrip (list->pg-array (list "abc")))
       (check-roundtrip (list->pg-array (list "a" "" "bc" "def" "")))
       (check-roundtrip (list->pg-array (list "a" sql-null "")))
       (check-=any "'abc'" (list) #f)
       (check-=any "'abc'" (list "abc" "def") #t)
       (check-=any "'abc'" (list "ab" "c" sql-null) #f))
    |#

    (type-test-case '(varchar-array)
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list "abc")))
      (check-roundtrip (list->pg-array (list "a" "" "bc" "def" "")))
      (check-roundtrip (list->pg-array (list "a" sql-null "")))
      (check-=any "'abc'" (list) #f)
      (check-=any "'abc'" (list "abc" "def") #t)
      (check-=any "'abc'" (list "abc" "def" sql-null) #t))

    (type-test-case '(bigint-array)
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list 1)))
      (check-roundtrip (list->pg-array (list 1 2 3)))
      (check-roundtrip (list->pg-array (list 1 sql-null 3 4 5)))
      (check-=any "1::bigint" (list) #f)
      (check-=any "1::bigint" (list 1 2 3) #t)
      (check-=any "1::bigint" (list sql-null 1 2 3) #t))

    (type-test-case '(point-array)
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list (point 1 2))))
      (check-roundtrip (list->pg-array (list (point 1 2) sql-null (point 3 4))))
      #| ;; no op= for point ?!
      (check-=any "POINT (1,2)" (list (point 1 2)) #t)
      (check-=any "POINT (1,2)" (list (point 3 4)) #f)
      (check-=any "POINT (1,2)" (list sql-null (point 1 2) (point 3 4)) #t)
      |#)

    (type-test-case '(real-array)
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list 1.0)))
      (check-roundtrip (list->pg-array (list 1.0 2.0 3.0)))
      (check-roundtrip (list->pg-array (list 1.0 sql-null 3.0 4.0 +inf.0)))
      (check-=any "1.0::real" (list) #f)
      (check-=any "1.0::real" (list 1.0) #t)
      (check-=any "1.0::real" (list sql-null 1.0 2.0 +inf.0) #t))

    (type-test-case '(double-array)
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list 1.0)))
      (check-roundtrip (list->pg-array (list 1.0 2.0 3.0)))
      (check-roundtrip (list->pg-array (list 1.0 sql-null 3.0 4.0 +inf.0)))
      (check-=any "1.0::float8" (list) #f)
      (check-=any "1.0::float8" (list 1.0) #t)
      (check-=any "1.0::float8" (list sql-null 1.0 2.0 +inf.0) #t))

    (type-test-case '(timestamp-array)
      (define ts1 (sql-timestamp 1999 12 31 11 22 33 0 #f))
      (define ts2 (sql-timestamp 2011 09 13 15 17 19 0 #f))
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list ts1)))
      (check-roundtrip (list->pg-array (list ts2 sql-null)))
      (check-=any "'1999-12-31 11:22:33'::timestamp" (list) #f)
      (check-=any "'1999-12-31 11:22:33'::timestamp" (list ts2) #f)
      (check-=any "'1999-12-31 11:22:33'::timestamp" (list ts1 sql-null ts2) #t))

    ;; FIXME: add timestamptz-array test (harder due to tz conversion)

    (type-test-case '(date-array)
      (define ts1 (sql-date 1999 12 31))
      (define ts2 (sql-date 2011 09 13))
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list ts1)))
      (check-roundtrip (list->pg-array (list ts2 sql-null)))
      (check-=any "'1999-12-31'::date" (list) #f)
      (check-=any "'1999-12-31'::date" (list ts2) #f)
      (check-=any "'1999-12-31'::date" (list ts1 sql-null ts2) #t))

    (type-test-case '(time-array)
      (define ts1 (sql-time 11 22 33 0 #f))
      (define ts2 (sql-time 15 17 19 0 #f))
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list ts1)))
      (check-roundtrip (list->pg-array (list ts2 sql-null)))
      (check-=any "'11:22:33'::time" (list) #f)
      (check-=any "'11:22:33'::time" (list ts2) #f)
      (check-=any "'11:22:33'::time" (list ts1 sql-null ts2) #t))

    ;; FIXME: add timetz-array test
    ;; FIXME: add interval-array test

    (type-test-case '(interval-array)
      (define i1 (sql-interval 0 0 3 4 5 6 0))
      (define i2 (sql-interval 87 1 0 0 0 0 0))
      (define i3 (sql-interval 1 2 3 4 5 6 45000))
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list i1)))
      (check-roundtrip (list->pg-array (list i2 sql-null i3)))
      (check-=any "'87 years 1 month'::interval" (list) #f)
      (check-=any "'87 years 1 month'::interval" (list i1 i3) #f)
      (check-=any "'87 years 1 month'::interval" (list i1 sql-null i2) #t))

    (type-test-case '(decimal-array)
      (define d1 12345678901234567890)
      (define d2 1/20)
      (define d3 +nan.0)
      (define d4 -100)
      (check-roundtrip (list->pg-array (list)))
      (check-roundtrip (list->pg-array (list d1)))
      (check-roundtrip (list->pg-array (list d2 sql-null d3 d4)))
      (check-=any "'0.05'::numeric" (list) #f)
      (check-=any "'0.05'::numeric" (list d1) #f)
      (check-=any "'0.05'::numeric" (list d1 sql-null d2 d3 d4) #t))

    ))
