#lang racket/base
(require racket/match
         racket/list
         racket/cmdline
         racket/file
         racket/place
         racket/future
         racket/runtime-path
         rackunit
         rackunit/text-ui
         racket/unit
         ffi/unsafe/os-thread
         db
         (only-in db/private/generic/ffi-common worker-modes)
         (only-in db/sqlite3)
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

  racket -l tests/db/all-tests -- -g <dsn> ...

== <dsn> ==

A <dsn> is turned into a symbol and looked up using 'get-dsn'. The
db-specific tests (which is most of them) are run using the resulting
data source. The data source must conform to the following rules:

 - It must include all necessary connection arguments (including the
   password, if one is required).

 - It should have an extension property named 'db:test whose value is
   a list. The list should contain exactly one of the following
   symbols, specifying the SQL dialect used by the data source: 'ispg,
   'ismy, 'issl, 'isora, 'isdb2, 'isfbd, 'isacc.

Running the tests on a new database system will almost certainly
require changing the test suite to generate the correct SQL dialect
and to selectively disable tests that can't run on a particular
system.
|#

;; ----------------------------------------

(define (get-dbconf name)
  (cond [(get-dsn name) => (lambda (r) (dbconf name r))]
        [else (error 'get-dbconf "no such dbconf: ~e" name)]))

(define (expand-dbconf x)
  (match x
    [(dbconf dbtestname (and r (data-source connector args exts)))
     (define flags (cond [(assq 'db:test exts) => cadr] [else null]))
     (define (ext-dbconf use-place new-flags)
       (dbconf (format "~a, use-place=~a" dbtestname use-place)
               (data-source connector (list* '#:use-place use-place args) (ext-add new-flags))))
     (define (ext-add new-flags)
       (cons (list 'db:test (append new-flags flags)) exts))
     (cond [(and (memq connector '(odbc odbc-driver sqlite3))
                 (not (memq '#:use-place args)))
            (append
             (list x)
             (for/list ([worker-mode (in-list worker-modes)])
               (ext-dbconf worker-mode
                           (case worker-mode
                             [(os-thread) '(async os-thread)]
                             [else '()])))
             (if (place-enabled?)
                 (list (ext-dbconf 'place '(async place)))
                 null))]
           [else (list x)])]))

(define (dbconf->unit the-dbconf flags0)
  (unit-from-context database^))

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
     (format "~a tests" (dbconf-name the-dbconf))
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

(define (make-all-tests dbconfs flags)
  (for/list ([dbconf (in-list dbconfs)])
    (specialize-test (dbconf->unit dbconf flags))))

;; ----------------------------------------

(define benchmark-iters 12)
(define-runtime-path testing-dsn-file "test-dsn.rktd")

(define (run #:gui? [gui? #f]
             #:include-sqlite? [include-sqlite? #f]
             #:add-use-place? [add-use-place? #f]
             #:testing-dsn-file? [testing-dsn-file? #t]
             #:flags [flags null]
             labels)
  (define dbconfs
    (append (if include-sqlite? (list sqlite3-dbconf) null)
            (parameterize ((current-dsn-file
                            (if testing-dsn-file?
                                testing-dsn-file
                                (current-dsn-file))))
              (map get-dbconf labels))))
  (define dbconfs2
    (cond [add-use-place? (apply append (map expand-dbconf dbconfs))]
          [else dbconfs]))
  (define tests (for/list ([dbconf dbconfs2])
                  (cons (format "~a" (dbconf-name dbconf))
                        (make-all-tests (list dbconf) flags))))
  (define (run proc) ;; returns void
    (cond [(memq 'use-parallel-thread flags)
           (thread-wait (thread #:pool 'own proc))]
          [else
           (thread-wait (thread proc))]))
  (define cust (make-custodian))
  (define ((wrap/custodian proc))
    (define sub-cust (make-custodian))
    (begin0 (parameterize ((current-custodian sub-cust)) (proc))
      (custodian-shutdown-all sub-cust)))
  (parameterize ((current-custodian cust))
    (cond [gui?
           (let* ([test/gui (dynamic-require 'rackunit/gui 'test/gui)])
             (apply test/gui #:wait? #t (append* (map cdr tests))))]
          [else
           (for ([test (in-list tests)])
             (define the-test (make-test-suite (car test) (cdr test)))
             (define (do-test) (run (lambda () (run-tests the-test))))
             (printf "Running ~s tests\n" (car test))
             (cond [(memq 'benchmark flags)
                    (define real-ms-list
                      (for/list ([i (in-range benchmark-iters)])
                        (define-values (_rs cpu-ms real-ms sys-ms)
                          (time-apply (wrap/custodian do-test) null))
                        (collect-garbage)
                        (printf "real time: ~s ms; memory use: ~s kb\n"
                                real-ms (quotient (current-memory-use) 1024))
                        real-ms))
                    (printf "~s ~s\n" (sort real-ms-list <)
                            ;; Drop highest and lowest, report sum of rest
                            (- (apply + real-ms-list)
                               (+ (apply min real-ms-list) (apply max real-ms-list))))]
                   [else
                    (time (run (lambda () (run-tests (make-test-suite (car test) (cdr test))))))])
             (newline))]))
  ;; The tests generally don't explicitly disconnect their connections. The
  ;; following encourages finalizer shutdown or forces custodian shutdown, useful
  ;; for testing with eg ASan (see racket/racket#3839).
  (when #t
    (for ([i 10]) (sync (system-idle-evt)) (collect-garbage)))
  (when #f
    (custodian-shutdown-all cust)))

;; ----------------------------------------

(module+ main
  (define flags null)
  (define gui? #f)
  (define include-sqlite? #f)
  (define add-use-place? #f)
  (command-line
   #:once-each
   [("--gui") "Run tests in RackUnit GUI" (set! gui? #t)]
   [("-t" "--testing-dsn-file") "Use testing DSN file" (current-dsn-file testing-dsn-file)]
   [("-s" "--sqlite3") "Run sqlite3 in-memory db tests" (set! include-sqlite? #t)]
   [("-p" "--add-use-place") "Add #:use-place variants" (set! add-use-place? #t)]
   [("--benchmark") "Run in benchmark mode"
                    (set! flags (cons 'benchmark flags))]
   [("--parallel") "Run tests in parallel thread"
                   (set! flags (cons 'use-parallel-thread flags))]
   [("--killsafe") "Wrap with kill-safe-connection"
                   (set! flags (cons 'kill-safe flags))]
   #:args labels
   (let ()
     (define no-labels? (not (or include-sqlite? (pair? labels))))
     (when no-labels? (set! add-use-place? #t))
     (when (and gui? (memq 'use-parallel-thread flags))
       (error 'all-tests "incompatible options: `--gui` and `--parallel`"))
     (when (and gui? (memq 'benchmark flags))
       (error 'all-tests "incompatible options: `--gui` and `--benchmark`"))
     (run #:gui? gui?
          #:include-sqlite? (or include-sqlite? no-labels?)
          #:add-use-place? add-use-place?
          #:flags flags
          #:testing-dsn-file? #f ;; already set above
          (map string->symbol labels)))))
