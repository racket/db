#lang setup/infotab

(define version "1.3")

(define collection 'multi)
(define deps '("srfi-lite-lib"
               ["base" #:version "6.90.0.24"]
               "unix-socket-lib"
               "sasl-lib"))

(define pkg-desc "implementation (no documentation) part of \"db\"")

(define pkg-authors '(ryanc))
