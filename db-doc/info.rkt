#lang info

(define version "1.2")

(define collection 'multi)

(define deps '("base"))

(define build-deps '("data-doc"
                     "srfi-lite-lib"
                     "web-server-doc"
                     "base"
                     "scribble-lib"
                     "sandbox-lib"
                     "web-server-lib"
                     ["db-lib" "1.2"]
                     "racket-doc"))
(define update-implies '("db-lib"))

(define pkg-desc "documentation part of \"db\"")

(define pkg-authors '(ryanc))
