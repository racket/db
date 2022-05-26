#lang racket/base
(require racket/contract/base
         db/private/generic/interfaces
         "private/geometry.rkt"
         (rename-in db/private/postgresql/util
                    [pg-custom-type make-custom-type]))

(define (pg-custom-type typeid name [base-type #f]
                        #:recv [recv-convert values]
                        #:send [send-convert values])
  (make-custom-type typeid name base-type recv-convert send-convert))

(provide/contract
 [pg-custom-type?
  (-> any/c boolean?)]
 [pg-custom-type
  (->* [exact-nonnegative-integer?
        symbol?]
       [(or/c #f symbol? exact-nonnegative-integer?)
        #:recv (or/c #f (procedure-arity-includes/c 1))
        #:send (or/c #f (procedure-arity-includes/c 1))]
       any)]
 [struct pg-box ([ne point?] [sw point?])]
 [struct pg-circle ([center point?] [radius (and/c real? (not/c negative?))])]
 [struct pg-path ([closed? any/c] [points (listof point?)])]

 [struct pg-array ([dimensions exact-nonnegative-integer?]
                   [dimension-lengths (listof exact-positive-integer?)]
                   [dimension-lower-bounds (listof exact-integer?)]
                   [contents vector?])]
 [pg-array-ref
  (->* (pg-array?) () #:rest (non-empty-listof exact-integer?) any)]
 [pg-array->list
  (-> pg-array? list?)]
 [list->pg-array
  (-> list? pg-array?)]

 [struct pg-empty-range ()]
 [struct pg-range ([lb any/c]
                   [includes-lb? boolean?]
                   [ub any/c]
                   [includes-ub? boolean?])]
 [pg-range-or-empty? (-> any/c boolean?)]
 [uuid? (-> any/c boolean?)])
