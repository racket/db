#lang racket/base
(require racket/class
         racket/tcp
         db/private/generic/interfaces
         db/private/generic/common
         "connection.rkt")
(provide cassandra-connect)

(define (cassandra-connect #:server [server "localhost"]
                           #:port [port 9042]
                           #:debug? [debug? #f])
  (define-values (in out) (tcp-connect server port))
  (define c (new connection% (inport in) (outport out)))
  (when debug? (send c debug #t))
  (send c start-connection-protocol)
  c)
