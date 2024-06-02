#lang racket/base
(require racket/os
         racket/port
         racket/string)

;; Link libssl.so, libcrypto.so before running test.
(require openssl)

#|
This program tests that open files are cleaned up when an exception is
raised during connection start.

`lsof' is required to count the number of open files.
|#

(define lsof (find-executable-path "lsof"))

(unless lsof
  (error "This test requires the lsof executable in PATH"))

(define (count-open-files)
  (parameterize ([current-subprocess-custodian-mode 'kill]
                 [current-custodian (make-custodian)])
    (call-with-continuation-barrier
     (lambda ()
       (dynamic-wind
         void
         (lambda ()
           (define-values (proc out in err)
             (subprocess #f #f #f lsof "-wnPp" (number->string (getpid))))
           (thread
            (lambda () (copy-port err (current-error-port))))
           (define lines (port->lines out))
           (printf "lsof:\n  ~a\n" (string-join lines "\n  "))
           (begin0 (sub1 (length lines))
             (subprocess-wait proc)
             (unless (equal? 0 (subprocess-status proc))
               (error "lsof returned non-zero status"))))
         (lambda ()
           (custodian-shutdown-all (current-custodian))))))))

(module+ main
  (require racket/tcp
           racket/match
           racket/cmdline
           racket/exn
           db)

  (define the-dsn (make-parameter 'pg))
  (define verbose? (make-parameter #f))

  (command-line
    #:once-each
    [("-v" "--verbose") "Enable verbose output" (verbose? #t)]
    #:args (dsn)
    (the-dsn (string->symbol dsn)))

  (define listener (tcp-listen 0 4 #f "127.0.0.1"))
  (match-define-values (_ listen-port _ _) (tcp-addresses listener #t))

  (define connection-attempts (box 0))
  (void
    (thread
      (lambda ()
        (let loop ()
          (define-values (in out) (tcp-accept listener))
          (close-input-port in)
          (close-output-port out)
          (set-box! connection-attempts (add1 (unbox connection-attempts)))
          (loop)))))

  (define (maybe-log-exn e)
    (when (verbose?)
      (printf "Exception in dsn-connect: ~a~n" (exn->string e))))

  (define open-files-start (count-open-files))
  (define expected-connection-attempts
    (for*/sum ([i 100]
               [ssl '(yes no)])
      (with-handlers
         ([exn:fail? maybe-log-exn])
        (dsn-connect (the-dsn) #:server "127.0.0.1" #:port listen-port #:ssl ssl))
      1))
  (define open-files-end (count-open-files))

  (unless (= (unbox connection-attempts) expected-connection-attempts)
    (error "expected number of connection attempts not made, try --verbose"))

  ;; Strict equality seems to work, given (require openssl) first.
  ;; May need to allow some slack in future if used dylibs change, etc.
  (unless (= open-files-end open-files-start)
    (printf "Started with ~a open files and ended with ~a, files are leaking~n"
            open-files-start
            open-files-end)
    (exit 1))

  (when #t
    (printf "Test passed\n")))

(module+ test)
