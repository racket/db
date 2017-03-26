#lang racket/base
(require racket/class
         racket/list
         racket/match
         racket/string
         file/sha1
         db/private/generic/interfaces
         db/private/generic/common
         db/private/generic/sql-data
         db/private/generic/sql-convert
         "message.rkt")
(provide dbsystem)

(define cassandra-dbsystem%
  (class* dbsystem-base% (dbsystem<%>)
    (define/public (get-short-name) 'cassandra)
    (define/override (get-type-list) type-list)

    (define/public (has-support? option)
      (case option
        ((real-infinities) #t)
        ((numeric-infinities) #t)
        (else #f)))

    (define/public (get-parameter-handlers param-typeids)
      (map make-type-writer param-typeids))

    (define/public (field-dvecs->typeids dvecs)
      (map dvec-type dvecs))

    (define/public (describe-params typeids)
      typeids)

    (define/public (describe-fields dvecs)
      (map dvec-type dvecs))

    (super-new)))

(define dbsystem (new cassandra-dbsystem%))

;; ============================================================

(define ((make-type-writer typeid) who v) (encode-value who typeid v))

(define type-list
  '(ascii
    bigint
    int
    blob
    boolean
    decimal
    double
    float
    ;; inet
    text
    varchar
    timestamp
    uuid
    timeuuid
    varint
    (list *)
    (set *)
    (map * *)
    (tuple **)))
