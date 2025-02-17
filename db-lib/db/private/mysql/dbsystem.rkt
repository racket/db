#lang racket/base
(require racket/class
         racket/match
         json
         db/private/generic/interfaces
         db/private/generic/common
         db/private/generic/sql-data
         "../../util/private/geometry.rkt"
         (submod "../../util/mysql.rkt" private)
         (only-in "message.rkt" length-code->bytes field-dvec->typeid field-dvec->flags))
(provide dbsystem
         classify-my-sql)

(define mysql-dbsystem%
  (class* dbsystem-base% (dbsystem<%>)

    (define/public (get-short-name) 'mysql)
    (define/override (get-type-list) type-list)

    (define/public (has-support? option)
      (case option
        ((real-infinities) #f)
        ((numeric-infinities) #f)
        (else #f)))

    (define/public (get-parameter-handlers param-typeids)
      ;; All params sent as binary data, so handled in message.rkt
      ;; Just need to check params for legal values here
      ;; FIXME: for now, only possible param type is var-string;
      ;; when that changes, will need to refine check-param.
      (map (lambda (param-typid) check-param)
           param-typeids))

    (define/public (field-dvecs->typeids dvecs)
      (map field-dvec->typeid dvecs))

    (define/public (describe-params typeids)
      (for/list ([_typeid (in-list typeids)])
        '(#t any #f)))

    (define/public (describe-fields dvecs)
      (for/list ([dvec (in-list dvecs)])
        (let ([r (describe-typeid (field-dvec->typeid dvec))])
          (match r
            [(list supported? type typeid)
             (let* ([binary? (memq 'binary (field-dvec->flags dvec))]
                    [type* (case type
                             ((tinyblob)   (if binary? type 'tinytext))
                             ((blob)       (if binary? type 'text))
                             ((mediumblob) (if binary? type 'mediumtext))
                             ((longblob)   (if binary? type 'longtext))
                             ((var-string) (if binary? 'var-binary type))
                             (else         type))])
               (if (eq? type* type)
                   r
                   (list supported? type* typeid)))]))))

    (super-new)))

(define dbsystem
  (new mysql-dbsystem%))


;; ========================================

(define DATE-YEAR-MIN 0)
(define DATE-YEAR-MAX 9999)

;; A CheckedParam is (cons type-symbol bytes-or-value)
;; Three variants, depending on the stage of parameter processing:
;; - v1, after check-param: All variable-length values are converted
;;   to bytes here, so that connection can decide what to send in long
;;   data packets.
;; - v2, after by connection sends long data: Payloads already sent as
;;   long-data are replaced by #f.

;; check-param : Symbol Any -> CheckParam-v1
;; Note: not applied to sql-null parameters.
(define (check-param fsym param)
  (cond [(string? param)
         (cons 'var-string (string->bytes/utf-8 param))]
        [(bytes? param)
         (cons 'blob param)]
        [(int64? param)
         (cons 'longlong param)]
        [(rational? param)
         (cons 'double param)]
        [(or (sql-time? param) (sql-day-time-interval? param))
         (cons 'time param)]
        [(sql-bits? param)
         (let-values ([(len bs start) (align-sql-bits param 'right)])
           (cons 'bit (bytes-append (length-code->bytes (- (bytes-length bs) start)) bs)))]
        [(geometry2d? param)
         ;; Since MySQL 8.0 (?), sending parameters as the 'geometry type does not work.
         ;; So send WKB as blob instead.
         (cons 'blob ;; 'geometry
               (geometry->bytes 'mysql-geometry->bytes param #:big-endian? #f #:srid? #f))]
        [(sql-date? param)
         (unless (<= DATE-YEAR-MIN (sql-date-year param) DATE-YEAR-MAX)
           (error/no-convert fsym "MySQL" "DATE" param "year out of range"))
         ;; Other ranges checked by sql-date contract at db/base.rkt
         (cons 'date param)]
        [(sql-timestamp? param)
         (unless (<= DATE-YEAR-MIN (sql-timestamp-year param) DATE-YEAR-MAX)
           (error/no-convert fsym "MySQL" "DATETIME" param "year out of range"))
         ;; See comment above for sql-date
         (cons 'timestamp param)]
        [(mysql-json? param)
         (cons 'json (mysql-json-bytes param))]
        ;; [(and (hash? param) (jsexpr? param))
        ;;  (cons 'json (jsexpr->bytes param))]
        [else
         (error/no-convert fsym "MySQL" "parameter" param)]))

;; ========================================

;; SQL "parsing"
;; We care about:
;;  - determining whether commands must be prepared (to use binary data)
;;    see http://dev.mysql.com/doc/refman/5.0/en/c-api-prepared-statements.html
;;  - determining what statements are safe for the statement cache
;;  - detecting commands that affect transaction status (maybe implicitly)
;;    see http://dev.mysql.com/doc/refman/5.0/en/implicit-commit.html

;; classify-my-sql : string [nat] -> symbol/#f
(define classify-my-sql
  (make-sql-classifier #:hash-comments? #t
   '(;; Must be prepared
     ("SELECT"            select)
     ("SHOW"              show)

     ;; Do not invalidate statement cache
     ("INSERT"            insert)
     ("DELETE"            delete)
     ("UPDATE"            update)

     ;; Explicit transaction commands
     ("ROLLBACK WORK TO"  rollback-savepoint)
     ("ROLLBACK TO"       rollback-savepoint)
     ("RELEASE SAVEPOINT" release-savepoint)
     ("SAVEPOINT"         savepoint)
     ("START TRANSACTION" start)
     ("BEGIN"             start)
     ("COMMIT"            commit)
     ("ROLLBACK"          rollback) ;; Note: after ROLLBACK TO, etc
     ("SET autocommit"    set-autocommit) ;; trouble
     ;; Note: commit/rollback may immediately start new transaction

     ;; Implicit commit
     ("ALTER"             implicit-commit)
     ("CREATE"            implicit-commit)
     ("DROP"              implicit-commit)
     ("RENAME"            implicit-commit)
     ("TRUNCATE"          implicit-commit)
     ("LOAD"              implicit-commit)
     ("LOCK TABLES"       implicit-commit)
     ("UNLOCK TABLES"     implicit-commit))))

;; ========================================

(define-type-table (type-list*
                    typeid->type
                    describe-typeid)

  (newdecimal  decimal     0)
  (tiny        tinyint     0)
  (short       smallint    0)
  (int24       mediumint   0)
  (long        integer     0)
  (longlong    bigint      0)
  (float       real        0)
  (double      double      0)
  (newdate     date        0)
  (time        time        0)
  (datetime    datetime    0)
  (varchar     varchar     0)
  (string      character   0)
  (var-string  var-string  0)
  (tiny-blob   tinyblob    0)
  (medium-blob mediumblob  0)
  (long-blob   longblob    0)
  (blob        blob        0)
  (bit         bit         0)
  (geometry    geometry    0)
  (json        json        5.7))

(define type-list
  (append (map (lambda (t) (list t 0))
               '(tinytext text mediumtext longtext var-binary))
          type-list*))

;; decimal, date typeids not used (?)
