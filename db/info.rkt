#lang info

(define collection 'multi)

(define deps
  '("db-lib" "db-doc" "base"))
(define implies
  '("db-lib" "db-doc"))

(define pkg-desc "Database connectivity")

(define pkg-authors '(ryanc))

(define license
  '(Apache-2.0 OR MIT))
