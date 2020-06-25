#lang setup/infotab

(define version "1.7")

(define collection 'multi)
(define deps '("srfi-lite-lib"
               ["base" #:version "7.7.0.9"]
               "unix-socket-lib"
               ["sasl-lib" #:version "1.1"]))

(define pkg-desc "implementation (no documentation) part of \"db\"")

(define pkg-authors '(ryanc))
