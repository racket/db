#lang setup/infotab

(define version "1.2")

(define collection 'multi)
(define deps '("srfi-lite-lib"
               ["base" #:version "6.2.900.17"]
               "unix-socket-lib"
               "sasl-lib"))

(define pkg-desc "implementation (no documentation) part of \"db\"")

(define pkg-authors '(ryanc))
