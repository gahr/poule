; Compute the first {n} powers of two using {k} processes.

(define n 50)
(define k 10)

(import (poule) (scheme) (chicken base) (srfi-1))
(let* ((p (poule-create (cut expt 2 <>) k))
       (j (list-tabulate n(cut poule-submit p <>)))
       (r (map (cut poule-result p <>) j))
       (_ (poule-destroy p)))
  (display r)
  (newline))
