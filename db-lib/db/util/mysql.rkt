#lang racket/base
(require racket/contract/base
         json)
(provide mysql-json?
         (contract-out
          [mysql-json
           (-> jsexpr? mysql-json?)]))

(module private racket/base
  (provide (struct-out mysql-json))
  (struct mysql-json (bytes) #:transparent))

(require (rename-in (submod "." private)
                    [mysql-json make-mysql-json]))

(define (mysql-json jsexpr)
  (make-mysql-json (jsexpr->bytes jsexpr)))
