#lang info

(define version "1.1")

(define collection 'multi)

(define deps
  '("db-lib" "db-doc" "base"))
(define implies
  '("db-lib" "db-doc"))

(define pkg-desc "Database connectivity")

(define pkg-authors '(ryanc))
