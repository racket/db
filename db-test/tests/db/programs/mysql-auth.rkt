#lang racket/base

;; Test different MySQL authentication mechanisms.

#|

REQUIREMENTS:

1. The MySQL server has users created as follows:

CREATE USER 'testauth_default'@'%' IDENTIFIED BY 'password';

CREATE USER 'testauth_native'@'%' IDENTIFIED WITH mysql_native_password BY 'password';
  -- on MySQL 5.6 and earlier:
  CREATE USER 'testauth_native'@'%' IDENTIFIED WITH mysql_native_password;
  SET PASSWORD FOR 'testauth_native'@'%' = PASSWORD('password');

-- only on MySQL 5.6 and earlier:
CREATE USER 'testauth_old'@'%' IDENTIFIED WITH mysql_old_password;
SET PASSWORD FOR 'testauth_old'@'%' = OLD_PASSWORD('password');

-- only on MySQL 8.0 and later:
CREATE USER 'testauth_caching_sha2'@'%' IDENTIFIED WITH caching_sha2_password BY 'password';

2. There is a Racket DSN (see dsn-connect, etc) pointing to the server
above with an extension named 'db:test-mysql-auth-plugins whose value
is a list of strings naming the plugins supported by the server. (Or
see "--all-plugins" option.)

|#

(module+ main
  (require db rackunit racket/cmdline)

  (define verbose? (make-parameter #f))
  (define the-dsn (make-parameter #f))
  (define the-avail-plugins (make-parameter null))

  (define (test-connect plugin user [password "password"])
    (test-case (format "~a connection, user ~s" plugin user)
      (cond [(or (not plugin) (member plugin (the-avail-plugins)))
             (when (verbose?)
               (eprintf "testing plugin ~a\n" (or plugin "(default)")))
             (define c (dsn-connect (the-dsn) #:user user #:password password #:debug? #f))
             (check-equal? (query-value c "select 'connected'") "connected")]
            [else
             (when (verbose?)
               (eprintf "skipping ~s, not available on server\n" plugin))])))

  (define (run-tests)
    (test-connect #f                      "testauth_default")
    (test-connect "mysql_native_password" "testauth_native")
    (test-connect "mysql_old_password"    "testauth_old")
    (test-connect "caching_sha2_password" "testauth_caching_sha2"))

  (let ()
    (define override-plugins? #f)
    (command-line
     #:once-each
     [("-v" "--verbose") "Enable verbose output" (verbose? #t)]
     [("--all-plugins") "Test all plugins, override DSN extension" (set! override-plugins? #t)]
     #:args (dsn)
     (let ([dsn (string->symbol dsn)])
       (define ds (get-dsn dsn))
       (unless ds (error 'mysql-auth-test "error: DSN not found: ~v" dsn))
       (define exts (if ds (data-source-extensions ds) null))
       (define avail-plugins
         (cond [override-plugins?
                '("mysql_native_password" "mysql_old_password" "caching_sha2_password")]
               [(assq 'db:test-mysql-auth-plugins exts) => cadr]
               [else (error 'mysql-auth-test
                            "no db:test-mysql-auth-plugins extension for DSN: ~v" dsn)]))
       (parameterize ((the-dsn dsn)
                      (the-avail-plugins avail-plugins))
         (run-tests))))))

(module+ test)
