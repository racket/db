#lang racket/base
(require rackunit
         racket/class
         ffi/unsafe
         ffi/unsafe/define
         db/private/odbc/ffi)

;; ----------------------------------------
;; UTF-16 and UTF-32 Support

(define (rt2 s) (mkstr2 (cpstr2 s)))
(define (rt4 s) (mkstr4 (cpstr4 s)))

(define some-strings
  `(""
    "\0"
    "abc"
    "abc\0def"
    "abc\0\0"
    "αβγ\0λω\0"
    ,(make-string 1000 #\A)
    ,(make-string 1000 (string-ref "λ" 0))))

(for ([s some-strings])
  (check-equal? (rt2 s) s)
  (check-equal? (rt4 s) s))

;; ----

(let ()
  (define-ffi-definer define-mz #f)
  (define-mz scheme_utf16_to_ucs4
    (_fun (src srcstart srcend) ::
          (src : _bytes)
          (srcstart : _intptr)
          (srcend : _intptr)
          (_pointer = #f)   ;; No buffer so it'll allocate for us.
          (_intptr = 0)
          (clen : (_ptr o _intptr))
          (_intptr = 0)
          -> (out : _gcpointer)
          -> (values out clen))
    #:fail (lambda () #f))
  (define-mz scheme_ucs4_to_utf16
    (_fun (src srcstart srcend) ::
          (src : _string/ucs-4)
          (srcstart : _intptr)
          (srcend : _intptr)
          (_pointer = #f)   ;; No buffer so it'll allocate for us.
          (_intptr = 0)
          (clen : (_ptr o _intptr))
          (_intptr = 0)
          -> (out : _gcpointer)
          -> (values out clen))
    #:fail (lambda () #f))
  (define (cpstr2* str)
    (let-values ([(shorts slen) (scheme_ucs4_to_utf16 str 0 (string-length str))])
      (define blen (* 2 slen))
      (define bs (make-bytes blen))
      (memcpy bs shorts blen)
      bs))
  (define (cpstr4* str)
    (define blen (* 4 (string-length str)))
    (define bs (make-bytes blen))
    (memcpy bs (cast str _string/ucs-4 _gcpointer) blen)
    bs)
  (define (mkstr2* buf len fresh?)
    (let-values ([(chars clen) (scheme_utf16_to_ucs4 buf 0 (quotient len 2))])
      (define s (make-string clen))
      (memcpy (cast s _string/ucs-4 _gcpointer) chars clen _uint32)
      s))
  (define (mkstr4* buf len fresh?)
    (define slen (quotient len 4))
    (define s (make-string slen))
    (memcpy (cast s _string/ucs-4 _gcpointer) buf slen _uint32)
    s)
  ;; ----

  (when (and scheme_ucs4_to_utf16 scheme_utf16_to_ucs4)
    (for ([s some-strings])
      (check-equal? (cpstr2 s) (cpstr2* s)))
    (for ([s some-strings])
      (check-equal? (cpstr2 s) (cpstr2* s))))
  (void))
  
