#lang setup/infotab

(define version "1.11")

(define collection 'multi)
(define deps '("srfi-lite-lib"
               ["base" #:version "8.15.0.2"]
               "unix-socket-lib"
               ["sasl-lib" #:version "1.1"]))

(define pkg-desc "implementation (no documentation) part of \"db\"")

(define pkg-authors '(ryanc))

(define license
  '(Apache-2.0 OR MIT))
