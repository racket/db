#lang racket/base

(provide (all-defined-out))

;; Exn:fail -> Boolean?
;;
;; Receives an exn:fail exception, performs a regex match on its message and returns true
;; if the exception does not represents the multiple statements given error.
(define (exn:fail-is-not-multiple-statements-error? exn-fail)
  (not (regexp-match? #rx"multiple statements given" (exn-message exn-fail))))
