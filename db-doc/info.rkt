#lang info

(define collection 'multi)

(define deps '("base"))

(define build-deps '("srfi-lite-lib"
                     "web-server-doc"
                     "base"
                     "scribble-lib"
                     "sandbox-lib"
                     "web-server-lib"
                     "db-lib"
                     "data-doc"
                     "racket-doc"))
(define update-implies '("db-lib"))

(define pkg-desc "documentation part of \"db\"")

(define pkg-authors '(ryanc))

(define license
  '(Apache-2.0 OR MIT))
