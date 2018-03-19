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
   ORFLAGS))

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

  (define dbsystem
    (with-handlers ([(lambda (e) #t)
                     (lambda (e) #f)])
      (let* ([c (connect)]
             [dbsystem (send c get-dbsystem)])
        (disconnect c)
        dbsystem)))

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
    [(odbc)       (add-flag! 'ffi)])

  ;; 'can-temp-table = can use "CREATE TEMP TABLE"; make temp "the_numbers" on connect
  ;; 'const-table = table "the_numbers" is pre-populated; don't modify!
  (if (ORFLAGS 'ispg 'ismy 'issl)
      (add-flag! 'can-temp-table)
      (add-flag! 'const-table))

  )
