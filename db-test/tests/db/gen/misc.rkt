#lang racket/base
(require rackunit
         racket/class
         db/private/generic/common)

;; ----------------------------------------
;; SQL "parsing"

(test-case "sql-skip-comments"
  (define (eat s [hash? #f])
    (substring s (sql-skip-comments s 0 #:hash-comments? hash?)))
  (check-equal? (eat "/* blah ** blah */ insert")
                " insert")
  (check-equal? (eat "-- blah\n  -- /* \nok")
                "ok")
  (check-equal? (eat "#a\n# b c d\nok" #t)
                "ok"))
