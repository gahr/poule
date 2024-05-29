; Find the multiples of {n} in the first {m} integers using {k} processes.
; This is done the dumb way, only to illustrate a parallel map / filter-map.

(define n 3)
(define m 500)
(define k 10)

(import (poule) (scheme) (chicken base) (srfi-1))
(let* ((p (poule-create (lambda (x) (zero? (remainder x n))) k))
       (n (iota m))
       (j (map (lambda (n) (cons n (poule-submit p n))) n))
       (r (filter-map (lambda (j) (and (poule-result p (cdr j)) (car j))) j))
       (_ (poule-destroy p)))
  (display r))
