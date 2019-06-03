#lang setup/infotab

(define version "1.6")

(define collection 'multi)
(define deps '("srfi-lite-lib"
               ["base" #:version "7.0.0.5"]
               "unix-socket-lib"
               "sasl-lib"))

(define pkg-desc "implementation (no documentation) part of \"db\"")

(define pkg-authors '(ryanc))
