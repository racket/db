#lang racket/base
(require racket/class
         ffi/unsafe/os-thread
         db/private/generic/interfaces
         db/private/generic/common
         db/private/generic/place-client
         "connection.rkt"
         "dbsystem.rkt"
         "ffi.rkt")
(provide odbc-connect
         odbc-driver-connect
         odbc-data-sources
         odbc-drivers)

(define (odbc-connect #:dsn dsn
                      #:user [user #f]
                      #:password [auth #f]
                      #:notice-handler [notice-handler void]
                      #:strict-parameter-types? [strict-parameter-types? #f]
                      #:character-mode [char-mode 'wchar]
                      #:quirks [quirks '()]
                      #:use-place [use-place #f])
  (let ([notice-handler (make-handler notice-handler "notice")]
        [worker-mode (normalize-worker-mode use-place)])
    (case worker-mode
      [(place)
       (place-connect (list 'odbc dsn user auth strict-parameter-types? char-mode quirks)
                      odbc-proxy%)]
      [else
       (define (connect db) (SQLConnect db dsn user auth))
       (new connection%
            (connect-info (list 'odbc-connect 'SQLConnect connect))
            (worker-mode worker-mode)
            (notice-handler notice-handler)
            (strict-parameter-types? strict-parameter-types?)
            (char-mode char-mode)
            (quirks quirks))])))

(define (odbc-driver-connect connection-string
                             #:notice-handler [notice-handler void]
                             #:strict-parameter-types? [strict-parameter-types? #f]
                             #:character-mode [char-mode 'wchar]
                             #:quirks [quirks '()]
                             #:use-place [use-place #f])
  (let ([notice-handler (make-handler notice-handler "notice")]
        [worker-mode (normalize-worker-mode use-place)])
    (case worker-mode
      [(place)
       (place-connect (list 'odbc-driver connection-string strict-parameter-types? char-mode quirks)
                      odbc-proxy%)]
      [else
       (define (connect db)
         (SQLDriverConnect db connection-string SQL_DRIVER_NOPROMPT))
       (new connection%
            (connect-info (list 'odbc-driver-connect 'SQLDriverConnect connect))
            (worker-mode worker-mode)
            (notice-handler notice-handler)
            (strict-parameter-types? strict-parameter-types?)
            (char-mode char-mode)
            (quirks quirks))])))

(define (normalize-worker-mode use-place)
  (cond [(eq? use-place #t)
         (if (os-thread-enabled?) 'os-thread 'place)]
        [else use-place]))

(define (odbc-data-sources)
  (call-with-env 'odbc-data-sources
    (lambda (env)
      (let loop ()
        (define-values (status name description) (SQLDataSources env SQL_FETCH_NEXT))
        (cond [(or (= status SQL_SUCCESS) (= status SQL_SUCCESS_WITH_INFO))
               (cons (list name description) (loop))]
              [else ;; SQL_NO_DATA
               (handle-status* 'odbc-data-sources (SQLFreeHandle SQL_HANDLE_ENV env))
               null])))))

(define (odbc-drivers)
  (call-with-env 'odbc-drivers
   (lambda (env)
     (let loop ()
       (define-values (status name attrs) (SQLDrivers env SQL_FETCH_NEXT))
       (cond [(or (= status SQL_SUCCESS) (= status SQL_SUCCESS_WITH_INFO))
              (cons (list name (parse-driver-attrs attrs)) (loop))]
             [else
              (handle-status* 'odbc-drivers (SQLFreeHandle SQL_HANDLE_ENV env))
              null])))))

(define (parse-driver-attrs buf)
  (let* ([attrs (regexp-split #rx"\0" buf)])
    (filter values
            (for/list ([s (in-list attrs)]
                       #:when (positive? (string-length s)))
              (let* ([m (regexp-match-positions #rx"=" s)])
                ;; Sometimes (eg iodbc on openbsd), returns ill-formatted attr-buf; just discard
                (and m
                     (let ([=-pos (caar m)])
                       (cons (substring s 0 =-pos) (substring s (+ 1 =-pos))))))))))

(define odbc-proxy%
  (class place-proxy-connection%
    (super-new)
    (define/override (get-dbsystem) dbsystem)))

;; Aux function to free handles on error.
(define (call-with-env fsym proc)
  (let-values ([(status env) (SQLAllocHandle SQL_HANDLE_ENV #f)])
    (with-handlers ([(lambda (e) #t)
                     (lambda (e)
                       (SQLFreeHandle SQL_HANDLE_ENV env)
                       (raise e))])
      (handle-status* fsym status env)
      (handle-status* fsym (SQLSetEnvAttr env SQL_ATTR_ODBC_VERSION SQL_OV_ODBC3))
      (proc env))))
