#lang racket/base
(require racket/contract/base
         db/base
         openssl
         "private/cassandra/main.rkt"
         "util/cassandra.rkt")

;; FIXME: Contracts duplicated at main.rkt
(provide/contract
 [cassandra-connect
  (->* []
       [#:server (or/c string? #f)
        #:port (or/c exact-positive-integer? #f)
        #:user (or/c string? #f)
        #:password (or/c string? #f)
        #:ssl (or/c 'yes 'no)
        #:ssl-context (or/c ssl-client-context? 'secure 'auto)
        #:debug? any/c]
       connection?)])
(provide cassandra-consistency)
