#lang racket/base
(require racket/contract/base
         (only-in "../private/cassandra/connection.rkt" cassandra-consistency)
         (only-in "../private/cassandra/message.rkt" consistency-symbols))

(provide/contract
 [cassandra-consistency
  (parameter/c (apply or/c consistency-symbols))])
