#lang info

(define collection 'multi)

(define deps '("base"
               "db-lib"
               "rackunit-lib"
               "web-server-lib"))

(define pkg-desc "tests for \"db\"")

(define pkg-authors '(ryanc))
(define build-deps '("srfi-lite-lib"))
(define update-implies '("db-lib"))

(define license
  '(Apache-2.0 OR MIT))
