#lang racket/base
(require racket/match
         racket/list
         racket/cmdline
         racket/file
         racket/place
         racket/runtime-path
         rackunit
         rackunit/text-ui
         racket/unit
         ffi/unsafe/os-thread
         db
         "config.rkt"
         (prefix-in db-
                    (combine-in "db/connection.rkt"
                                "db/query.rkt"
                                "db/sql-types.rkt"
                                "db/special.rkt"
                                "db/concurrent.rkt")))
(provide (all-defined-out))

#|

RUNNING THE TESTS
-----------------

0) To run the generic tests (ie, those that don't require a db
connection):

  raco test -c tests/db/gen

1) Default test configuration.

To run the default tests (ie, the sqlite3 tests),
simply execute this file with no arguments:

  racket -l tests/db/all-tests

This is how DrDr runs the file---we assume the machine running DrDr
has sqlite installed.

2) Custom test configuration.

First, set up the testing environment as described in the following
subsections.

Then, run the tests with the following command line:

  racket -l tests/db/all-tests -- -g <label> ...

Each <label> is either a <dsn> or a <confname>.

== <dsn> ==

A <dsn> is turned into a symbol and looked up using 'get-dsn'. The
db-specific tests (which is most of them) are run using the resulting
data source. The data source must conform to the following rules:

 - It must include all necessary connection arguments (including the
   password, if one is required).

 - It should have an extension property named 'db:test whose value is
   a list. The list should contain exactly one of the following
   symbols, specifying the SQL dialect used by the data source: 'ispg,
   'ismy, 'issl, 'isora, 'isdb2.

 - If either 'isora or 'isdb2 is set, the database should already be
   populated with the "the_numbers" table (see config.rkt for definition
   and test data). Otherwise, it is automatically created as a temporary
   table for each test case.

Running the tests on a new database system will almost certainly
require changing the test suite to generate the correct SQL dialect
and to selectively disable tests that can't run on a particular
system.

== <confname>: Defining testing configurations ==

The "db-test.rktd" pref file (see definition of 'pref-file' below
gives a mapping from <confnames> (ie, symbols) to <conf>s, which
aggregate multiple testing data sources into a single profile.

<conf> ::= (profile <conf> ...)
         | (dsn <symbol>)
         | (ref <symbol>)

Testing profiles are flattened, not hierarchical.

|#

;; ----------------------------------------

(define pref-file
  (make-parameter (build-path (find-system-path 'pref-dir) "db-test.rktd")))

(define (get-dbconf name)
  (let ([conf (get-preference name (lambda () #f) 'timestamp (pref-file))])
    (if conf
        (parse-dbconf conf)
        (let ([r (get-dsn name)])
          (if r
              (list (dbconf name r))
              (error 'get-dbconf "no such dbconf: ~e" name))))))

(struct dbconf (name dsn) #:transparent)

(define-syntax-rule (expect name pred)
  (unless (pred name) (error 'parse "bad ~a: ~e" 'name name)))

;; parse-dbconf : sexpr -> (listof dbconf?)
(define (parse-dbconf x)
  (match x
    [(list 'profile dbconfs ...)
     (apply append (map parse-dbconf dbconfs))]
    [(list 'ref conf-name)
     (expect conf-name symbol?)
     (get-dbconf conf-name)]
    [(list 'dsn dsn-name)
     (expect dsn-name symbol?)
     (list (dbconf dsn-name (get-dsn dsn-name)))]))

;; ----

;; Set below by command-line parsing
(define kill-safe? #f)
(define add-use-place? #f)

(define (expand-dbconf x)
  (match x
    [(dbconf dbtestname (and r (data-source connector args exts)))
     (define (ext-add new-vs)
       (define vs
         (cond [(assq 'db:test exts) => (match-lambda [(list _ vs) vs])]
               [else null]))
       (cons (list 'db:test (append new-vs vs)) exts))
     (cond [(and (memq connector '(odbc odbc-driver sqlite3))
                 (not (memq '#:use-place args)))
            (append
             (list x)
             (if (os-thread-enabled?)
                 (list (dbconf (format "~a, use-place=os-thread" dbtestname)
                               (data-source connector
                                            (list* '#:use-place 'os-thread args)
                                            (ext-add '(async os-thread)))))
                 null)
             (if (place-enabled?)
                 (list (dbconf (format "~a, use-place=place" dbtestname)
                               (data-source connector
                                            (list* '#:use-place 'place args)
                                            (ext-add '(async place)))))
                 null))]
           [else (list x)])]))

(define (dbconf->unit x)
  (match x
    [(dbconf dbtestname (and r (data-source connector _args exts)))
     (define (connect) (dsn-connect r))
     (define dbsys (case connector ((odbc-driver) 'odbc) (else connector)))
     (define dbflags (cond [(assq 'db:test exts) => cadr] [else '()]))
     (define (connect-first-time ssl?)
       (if ssl? (dsn-connect #:ssl 'yes r) (connect)))
     (unit-from-context database^)]))

(define sqlite3-dbconf
  (dbconf "sqlite3, memory"
          (data-source 'sqlite3 '(#:database memory) '((db:test (issl))))))

;; ----

(define-unit db-test@
  (import database^
          (tag connect (prefix connect: test^))
          (tag query (prefix query: test^))
          (tag sql-types (prefix sql-types: test^))
          (tag special (prefix special: test^))
          (tag concurrent (prefix concurrent: test^)))
  (export test^)
  (define test
    (make-test-suite
     (format "~a tests" dbtestname)
     (list connect:test
           query:test
           sql-types:test
           special:test
           concurrent:test))))

(define (specialize-test@ db@)
  (compound-unit
   (import)
   (export DB-TEST)
   (link (((DB : database^)) db@)
         (((CONFIG : config^)) config@ DB)
         (((CONNECT-TEST : test^)) db-connection@ CONFIG DB)
         (((QUERY-TEST : test^)) db-query@ CONFIG DB)
         (((SQL-TYPES-TEST : test^)) db-sql-types@ CONFIG DB)
         (((SPECIAL-TEST : test^)) db-special@ CONFIG DB)
         (((CONCURRENT-TEST : test^)) db-concurrent@ CONFIG DB)
         (((DB-TEST : test^)) db-test@
                               DB
                               (tag connect CONNECT-TEST)
                               (tag query QUERY-TEST)
                               (tag sql-types SQL-TYPES-TEST)
                               (tag special SPECIAL-TEST)
                               (tag concurrent CONCURRENT-TEST)))))

(define (specialize-test db@)
  (define-values/invoke-unit (specialize-test@ db@) (import) (export test^))
  test)

;; ----

(define (make-all-tests dbconfs)
  (for/list ([dbconf (in-list dbconfs)])
    (specialize-test (dbconf->unit dbconf))))

;; ----

(define-syntax-rule (setup-debug db@ c)
  (begin (define-values/invoke-unit db@ (import) (export database^))
         (define-values/invoke-unit config@ (import database^) (export config^))
         (define c (connect-and-setup))))

;; ----------------------------------------

(define gui? #f)
(define include-sqlite? #f)
(define use-parallel-thread? #f)

(define-runtime-path testing-dsn "test-dsn.rktd")

(define cust (make-custodian))

(command-line
 #:once-each
 [("--gui") "Run tests in RackUnit GUI" (set! gui? #t)]
 [("-k" "--killsafe") "Wrap with kill-safe-connection" (set! kill-safe? #t)]
 [("-s" "--sqlite3") "Run sqlite3 in-memory db tests" (set! include-sqlite? #t)]
 [("-f" "--config-file") file  "Use configuration file" (pref-file file)]
 [("-p" "--add-use-place") "Add #:use-place variants" (set! add-use-place? #t)]
 [("-t" "--testing-dsn-file") "Use testing DSN file" (current-dsn-file testing-dsn)]
 [("--parallel") "Run tests in parallel thread" (set! use-parallel-thread? #t)]
 #:args labels
 (let ()
   (define no-labels? (not (or include-sqlite? (pair? labels))))
   (when no-labels? (set! add-use-place? #t))
   (when (and gui? use-parallel-thread?)
     (error 'all-tests "incompatible options: `--gui` and `--parallel`"))
   (define dbconfs
     (append (if (or include-sqlite? no-labels?)
                 (list sqlite3-dbconf)
                 null)
             (append*
              (for/list ([label labels])
                (get-dbconf (string->symbol label))))))
   (define dbconfs2
     (cond [add-use-place? (apply append (map expand-dbconf dbconfs))]
           [else dbconfs]))
   (define tests (for/list ([dbconf dbconfs2])
                   (cons (format "~a" (dbconf-name dbconf))
                         (make-all-tests (list dbconf)))))
   (define (run proc) ;; returns void
     (cond [use-parallel-thread?
            (thread-wait (thread #:pool 'own proc))]
           [else (proc)]))
   (parameterize ((current-custodian cust))
     (cond [gui?
            (let* ([test/gui (dynamic-require 'rackunit/gui 'test/gui)])
              (apply test/gui #:wait? #t (append* (map cdr tests))))]
           [else
            (for ([test tests])
              (printf "Running ~s tests\n" (car test))
              (time (run (lambda () (run-tests (make-test-suite (car test) (cdr test))))))
              (newline))]))))

;; The tests generally don't explicitly disconnect their connections. The
;; following encourages finalizer shutdown or forces custodian shutdown, useful
;; for testing with eg ASan (see racket/racket#3839).
(when #t
  (for ([i 10]) (sync (system-idle-evt)) (collect-garbage)))
(when #f
  (custodian-shutdown-all cust))
