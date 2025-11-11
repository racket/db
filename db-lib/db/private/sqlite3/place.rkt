#lang racket/base
(require racket/contract/base
         racket/class
         ffi/unsafe/os-thread
         db/base
         db/private/generic/place-client
         (rename-in db/private/sqlite3/main
                    [sqlite3-connect pre:sqlite3-connect])
         db/private/sqlite3/dbsystem)

(define (sqlite3-connect #:database path
                         #:mode [mode 'read/write]
                         #:busy-retry-delay [busy-retry-delay 0.1]
                         #:busy-retry-limit [busy-retry-limit 10]
                         #:debug? [debug? #f]
                         #:use-place [use-place #f])
  (define (connect)
    (pre:sqlite3-connect #:database path
                         #:mode mode
                         #:busy-retry-delay busy-retry-delay
                         #:busy-retry-limit busy-retry-limit
                         #:debug? debug?))
  (let ([use-place
         (cond [(eq? use-place #t)
                (if (os-thread-enabled?) 'os-thread 'place)]
               [else use-place])])
    (case use-place
      [(place)
       (place-connect (list 'sqlite3 path mode busy-retry-delay busy-retry-limit)
                      sqlite-place-proxy%)]
      [else
       (define c (connect))
       (send c use-worker-mode use-place)
       c])))

(define sqlite-place-proxy%
  (class place-proxy-connection%
    (super-new)
    (define/override (get-dbsystem) dbsystem)))

(provide sqlite3-connect
         sqlite3-available?)
