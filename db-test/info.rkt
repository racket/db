#lang info

(define version "1.3")

(define collection 'multi)

(define deps '("base"
               ["db-lib" "1.3"]
               "rackunit-lib"
               "web-server-lib"))

(define pkg-desc "tests for \"db\"")

(define pkg-authors '(ryanc))
(define build-deps '("srfi-lite-lib"))
(define update-implies '("db-lib"))
