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

  (define (connect-and-setup)
    (when (FLAG 'sleep-before-connect)
      ;; Oracle ODBC tests spuriously fail if connections are made too quickly
      (sleep 0.01))
    (let [(cx (connect-for-test))]
      (when (FLAG 'can-temp-table)
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

  (define (call-with-connection f)
    (let [(c (connect-and-setup))]
      (dynamic-wind void
                    (lambda () (f c))
                    (lambda () (disconnect c)))))

  (define (sql str)
    (case dbsys
      ((postgresql) str)
      ((mysql sqlite3 odbc) (regexp-replace* #rx"\\$[0-9]" str "?"))
      (else (error 'sql "unsupported dbsystem: ~e" dbsys))))

  (define (select-val str)
    (cond [(FLAG 'isora)
           (sql (string-append "select " str " from DUAL"))]
          [(FLAG 'isdb2)
           (sql (string-append "values (" str ")"))]
          [else (sql (string-append "select " str))]))

  ;; ----------------------------------------
  ;; Flags

  ;; Standard flags:
  ;;   connection = 'postgresql | 'mysql | 'sqlite3 | 'odbc
  ;;   impl type  = 'wire | 'ffi
  ;;   dialect    = 'ispg | 'ismy | 'issl
  ;;              | 'isdb2 | 'isora (oracle) | 'ismss (MS SQL Server)

  ;; Other flags (set in data-source extensions under db:test key):
  ;;   'async = ODBC connection allows async execution (ie, via places)
  ;;   'pg92  = PostgreSQL >= 9.2
  ;;   'concurrent = ok to test many concurrent connections
  ;;   'sleep-before-connect = ad hoc rate-limiting for connections
  ;;   'connect-first-ssl = add { #:ssl 'yes } to first connection attempt
  ;;                        (eg to warm up MySQL 8 sha2 auth cache)
  ;;   'keep-unused-connection = keep an unused connection open throughout tests
  ;;                             (eg DB2 time goes from 150s to 0.6s)

  (define allflags (cons dbsys dbflags))
  (define (add-flag! . fs) (set! allflags (append fs allflags)))

  (define (FLAG x) (and (member x allflags) #t))
  (define (ORFLAGS . xs) (ormap FLAG xs))
  (define (ANDFLAGS . xs) (andmap FLAG xs))

  ;; -- Derived flags --

  (case dbsys
    [(postgresql) (add-flag! 'ispg 'wire 'concurrent 'async)]
    [(mysql)      (add-flag! 'ismy 'wire 'concurrent 'async)]
    [(sqlite3)    (add-flag! 'issl 'ffi  'concurrent)]
    [(odbc)       (add-flag! 'ffi)]
    [(cassandra)  (error 'config "this test suite does not support Cassandra")])

  ;; 'can-temp-table = can use "CREATE TEMP TABLE"; make temp "the_numbers" on connect
  ;; 'const-table = table "the_numbers" is created below; don't modify in query tests!
  (if (ORFLAGS 'ispg 'ismy 'issl)
      (add-flag! 'can-temp-table)
      (add-flag! 'const-table))

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
        (cond [(ORFLAGS 'ismss) ;; table-exists? doesn't work
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
