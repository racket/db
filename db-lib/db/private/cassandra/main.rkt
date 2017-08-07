#lang racket/base
(require racket/class
         racket/tcp
         openssl
         db/private/generic/interfaces
         db/private/generic/common
         "connection.rkt")
(provide cassandra-connect)

(define (cassandra-connect #:server [server "localhost"]
                           #:port [port 9042]
                           #:user [user #f]
                           #:password [password #f]
                           #:ssl [ssl 'no]
                           #:ssl-context [ssl-context 'auto]
                           #:debug? [debug? #f])
  (define-values (in out)
    (case ssl
      [(no) (tcp-connect server port)]
      [(yes) (ssl-connect server port ssl-context)]))
  (define c (new connection% (inport in) (outport out)))
  (when debug? (send c debug #t))
  (send c start-connection-protocol user password)
  c)
