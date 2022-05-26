#lang racket/base
(require racket/contract/base
         db/private/generic/interfaces
         "private/geometry.rkt"
         db/private/postgresql/util)

(provide/contract
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
