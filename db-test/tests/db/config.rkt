#lang racket/base
(require racket/class
         racket/unit
         db/base)
(provide database^
         test^
         config^
         config@)

(define-signature database^
  (dbtestname
   connect
   connect-first-time
   dbsys
   dbflags
   kill-safe?))

(define-signature test^ (test))
(define-signature config^
  (connect-for-test
   connect-and-setup
   call-with-connection
   (define-syntaxes (with-connection)
     (syntax-rules ()
       [(with-connection c . body)
        (call-with-connection (lambda (c) . body))]))
   test-data
   set-equal?
   sql
   select-val
   dbsystem
   NOISY?
   FLAG
   ANDFLAGS
   ORFLAGS
   config-dont-gc))

(define-unit config@
  (import database^)
  (export config^)

  (define NOISY? #f)

  (define (connect-for-test)
    (cond [kill-safe? (kill-safe-connection (connect))]
          [else (connect)]))

  (define test-data
    '((0 "nothing")
      (1 "unity")
      (2 "the loneliest number since the number one")
      (4 "four")
      (5 "five")
      (6 "half a dozen")))

  (define (connect-and-setup [setup? #t])
    (when (FLAG 'sleep-before-connect)
      ;; Oracle ODBC tests spuriously fail if connections are made too quickly
      (sleep 0.02))
    (let [(cx (connect-for-test))]
      (when (and setup? (FLAG 'can-temp-table))
        (query-exec cx
          "create temporary table the_numbers (N integer primary key, descr varchar(80))")
        (for-each (lambda (p)
                    (query-exec cx
                                (format "insert into the_numbers values (~a, '~a')"
                                        (car p) (cadr p))))
                  test-data))
      cx))

  ;; set-equal? : ('a list) ('a list) -> boolean
  (define (set-equal? a b)
    (and (andmap (lambda (xa) (member xa b)) a)
         (andmap (lambda (xb) (member xb a)) b)
         #t))

  (define (call-with-connection f [setup? #t])
    (let [(c (connect-and-setup setup?))]
      (dynamic-wind void
                    (lambda () (f c))
                    (lambda () (disconnect c)))))

  (define (sql str)
    (case dbsys
      ((postgresql) str)
      ((mysql sqlite3 odbc) (regexp-replace* #rx"\\$[0-9]" str "?"))
      (else (error 'sql "unsupported dbsystem: ~e" dbsys))))

  (define (select-val str)
    (cond [(not str) #f]
          [(FLAG 'isora)
           (sql (string-append "select " str " from DUAL"))]
          [(FLAG 'isdb2)
           (sql (string-append "values (" str ")"))]
          [(FLAG 'isfbd)
           (sql (string-append "select " str " from RDB$DATABASE"))]
          [else (sql (string-append "select " str))]))

  ;; ----------------------------------------
  ;; Flags

  ;; Standard flags:
  ;;   connection = 'postgresql | 'mysql | 'sqlite3 | 'odbc
  ;;   impl type  = 'wire | 'ffi
  ;;   dialect    = 'ispg | 'ismy | 'issl
  ;;              | 'isdb2 | 'isora (oracle) | 'ismss (MS SQL Server)
  ;;              | 'isfbd (Firebird)

  ;; Other flags (set in data-source extensions under db:test key):
  ;;   'async = ODBC connection allows async execution (ie, via places)
  ;;   'pg92  = PostgreSQL >= 9.2
  ;;   'concurrent = ok to test many concurrent connections
  ;;   'sleep-before-connect = ad hoc rate-limiting for connections
  ;;   'connect-first-ssl = add { #:ssl 'yes } to first connection attempt
  ;;       - used to warm up MySQL 8 sha2 auth cache
  ;;   'keep-unused-connection = keep an unused connection open throughout tests
  ;;       - avoids reconnect overhead; DB2 time goes from 150s to 0.6s
  ;;   'place, 'os-thread = use-place mode (for ffi-based connection))
  ;;   'can-temp-table = driver supports "CREATE TEMPORARY TABLE"
  ;;       - use temp table for the_numbers, for type roundtrip tests
  ;;   'const-table = do not modify the_numbers test table (not temporary)
  ;;       - test table is persistent, not temp per-test
  ;;   'rt-table = use persistent table for type roundtrips (if cannot use temp)
  ;;       - always set unless DSN has 'no-rt-table flag
  ;;   'no-interleave = connection cannot interleave queries
  ;;       - eg, MS SQL Server w/o MARS
  ;;   'no-{time,date,text} = skip specified type tests

  (define allflags (cons dbsys dbflags))
  (define (add-flag! . fs) (set! allflags (append fs allflags)))

  (define (FLAG x) (and (member x allflags) #t))
  (define (ORFLAGS . xs) (ormap FLAG xs))
  (define (ANDFLAGS . xs) (andmap FLAG xs))

  ;; ----------------------------------------
  ;; Derived flags

  (case dbsys
    [(postgresql) (add-flag! 'ispg 'wire 'concurrent 'async)]
    [(mysql)      (add-flag! 'ismy 'wire 'concurrent 'async)]
    [(sqlite3)    (add-flag! 'issl 'ffi  'concurrent)]
    [(odbc)       (add-flag! 'ffi)]
    [(cassandra)  (error 'config "this test suite does not support Cassandra")])

  (cond [(ORFLAGS 'ispg 'ismy 'issl)
         (add-flag! 'can-temp-table)]
        [else
         (add-flag! 'const-table)])

  (when (FLAG 'isora)
    (add-flag! 'no-date 'no-time)
    ;; Tests sometimes spuriously fail if connections are made too quickly;
    ;; use sleep-before-connect to work around.
    (add-flag! 'sleep-before-connect))

  (when (FLAG 'isdb2)
    ;; Note: tests with keep-unused connection <1s; without ~150s
    (add-flag! 'keep-unused-connection))

  (unless (FLAG 'no-rt-table)
    (add-flag! 'rt-table))

  ;; ----------------------------------------

  (define dbsystem
    (with-handlers ([exn:fail?
                     (lambda (e)
                       (eprintf "ERROR: Connection failed!\n")
                       (eprintf "Check DSN for test: name=~e, sys=~e, flags=~e.\n"
                                dbtestname dbsys dbflags)
                       (raise e))])
      (define c (connect-first-time (FLAG 'connect-first-ssl)))
      (when (FLAG 'const-table)
        ;; table-exists? doesn't work on SQLServer
        (cond [(ORFLAGS 'ismss 'isfbd) ;; table-exists? doesn't work
               (with-handlers ([exn:fail? void])
                 (query-exec c "drop table the_numbers"))]
              [else
               (when (table-exists? c "the_numbers")
                 (query-exec c "drop table the_numbers"))])
        ;; DB2: "primary key" apparently doesn't imply "not null"
        (cond [(FLAG 'isdb2)
               (query-exec c
                 "create table the_numbers (N integer not null primary key, descr varchar(80))")]
              [else
               (query-exec c "create table the_numbers (N integer primary key, descr varchar(80))")])
        (for ([p test-data])
          (query-exec c (format "insert into the_numbers values (~a, '~a')" (car p) (cadr p)))))
      (begin0 (send c get-dbsystem)
        (disconnect c))))

  (define config-dont-gc
    (cond [(FLAG 'keep-unused-connection)
           (connect)]
          [else (void)])))
