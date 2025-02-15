#lang racket/unit
(require racket/class
         rackunit
         "../config.rkt"
         db/base
         db/util/postgresql)
(import database^ config^)
(export test^)

(define-syntax-rule (with-connection c . body)
  (call-with-connection (lambda (c) . body)))

;; ----------------------------------------

(define test
  (test-suite "special APIs"

    (when (FLAG 'postgresql)
      (test-case "add-custom-types: inet built-in"
        (with-connection c
          (send c add-custom-types
                (list (pg-custom-type 869 'inet
                                      #:recv bytes->list
                                      #:send list->bytes
                                      #:array 1041)))
          (check-equal? (query-value c "select inet '127.0.0.1/32'")
                        '(2 32 0 4 127 0 0 1))
          (check-equal? (query-value c "select host($1::inet)" '(2 32 0 4 127 0 0 1))
                        "127.0.0.1")
          (check-equal?
           (pg-array->list
            (query-value c "select cast('{127.0.0.1/32, 10.0.0.1/8}' as inet[])"))
           '((2 32 0 4 127 0 0 1) (2 8 0 4 10 0 0 1)))))

      (test-case "add-custom-types: domain"
        (with-connection c
          (query-exec c "drop domain if exists zipcode")
          (query-exec c "create domain zipcode as char(5) check (value similar to '[0-9]{5}')")
          (define-values (zipcode-oid zipcode-array-oid)
            (vector->values
             (query-row c "select oid, typarray from pg_type where typname = 'zipcode'")))
          (send c add-custom-types
                (list (pg-custom-type zipcode-oid 'zipcode 'text #:array zipcode-array-oid)))
          ;; For domains, results seem to be returned as base type anyway.
          (check-equal? (query-value c "select zipcode '12345'") "12345")
          (check-equal? (pg-array->list
                         (query-value c "select cast('{12345, 23456}' as zipcode[])"))
                        '("12345" "23456"))
          ;; Need custom type for parameters, though.
          (check-equal? (query-value c "select cast($1::zipcode as text)" "12345") "12345")
          (check-equal?
           (pg-array->list (query-value c "select ($1::zipcode[])" '("12345" "23456")))
           '("12345" "23456"))
          (check-exn exn:fail?
                     (lambda () (query-value c "select cast($1::zipcode as text)" "123")))))

      (test-case "add-custom-types: composite (record) type"
        (with-connection c
          (query-exec c "drop type if exists intpair")
          (query-exec c "create type intpair as (car integer, cdr integer)")
          (define-values (typeid array-typeid)
            (vector->values
             (query-row c "select oid, typarray from pg_type where typname = 'intpair'")))
          (send c add-custom-types
                (list (pg-custom-type typeid 'intpair #:array array-typeid)))
          ;; Type is necessary both for parameters and results.
          (define val (query-value c "select intpair '(123,456)'"))
          (check-pred bytes? val)
          (check-equal? (query-value c "select $1::intpair" val) val)
          (check-equal? (query-value c "select ($1::intpair).car" val) 123)
          (check-equal? (pg-array->list
                         (query-value c "select $1::intpair[]" (list val val)))
                        (list val val))))

      (test-case "postgresql-cancel: noop when there are no queries in progress"
        (with-connection c
          (send c cancel)
          (check-equal? (query-value c "select 1") 1)))

      (test-case "postgresql-cancel: cancels queries in progress"
        (with-connection c
          (define ch
            (make-channel))
          (thread
           (lambda ()
             (with-handlers ([exn:fail? (λ (e) (channel-put ch e))])
               (query-exec c "select pg_sleep(10)")
               (channel-put ch 'fail))))
          (sync (system-idle-evt))
          (send c cancel)
          (define e (channel-get ch))
          (check-true (exn:fail:sql? e))
          (check-regexp-match #rx"canceling statement due to user request" (exn-message e)))))

    ))
