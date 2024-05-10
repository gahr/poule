(import
  (chicken base)
  (chicken condition)
  (chicken file)
  (chicken format)
  (chicken io)
  (chicken process-context posix)
  (chicken random)
  (poule)
  (test)
  (matchable)
  (srfi-1)
  (srfi-18))

(poule-trace #f)

(define (sleep-some)
  (let ((s (exact->inexact (/ (+ 50 (pseudo-random-integer 100)) 1000))))
    (thread-sleep! s)))

(define (sleepy-worker item)
  (sleep-some)
  "done")

(define (make-temporary-directory-worker d)
  (lambda (item)
    (with-output-to-file (sprintf "~A/~A" d item) (lambda () (sleepy-worker item)))
    #t))

(define math-worker
  (match-lambda
    (('add x y)
     (sleep-some)
     (+ x y))
    (('sub x y)
     (sleep-some)
     (- x y))))

(test-group "create errors"
  (test-error "invalid fn" (pool-create #f 0))
  (test-error "not a number" (pool-create sleepy-worker #f))
  (test-error "negative number" (pool-create sleepy-worker -3)))

(test-group "lifetime"
  (test-assert "poule-destroy w/ implicit wait"
    (poule-destroy (poule-create sleepy-worker 2)))
  (test-assert "poule-destroy w/ explicit wait"
    (poule-destroy (poule-create sleepy-worker 2) #t))
  (test-assert "poule-destroy w/ explicit no-wait"
    (poule-destroy (poule-create sleepy-worker 2) #f)))

(test-group "submission"
  (define data (iota 10))
  (test "1 process"
    (length data)
    (begin
      (let* ((d (create-temporary-directory))
             (p (poule-create (make-temporary-directory-worker d) 1)))
        (for-each (cut poule-submit p <>) data)
        (poule-destroy p #t)
        (let ((entries (length (glob (sprintf "~A/*" d)))))
          (delete-directory d #t)
          entries))))
  (test "on 5 process"
    (length data)
    (begin
      (let* ((d (create-temporary-directory))
             (p (poule-create (make-temporary-directory-worker d) 5)))
        (for-each (cut poule-submit p <>) data)
        (poule-destroy p #t)
        (let ((entries (length (glob (sprintf "~A/*" d)))))
          (delete-directory d #t)
          entries)))))

(test-group "result"
  (test "wrong number"
    #f
    (let* ((p (poule-create (lambda (x) x) 1))
           (r (poule-result p 12)))
      (poule-destroy p)
      r))
  (test-assert "math"
    (let* ((p (poule-create math-worker 10))
           (n (list-tabulate 100 (lambda _
                                   (list (pseudo-random-integer 100)
                                         (pseudo-random-integer 100)))))
           (adds (map (lambda (pair) (poule-submit p `(add ,@pair))) n))
           (subs (map (lambda (pair) (poule-submit p `(sub ,@pair))) n))
           (res (every
                  (lambda (in add sub)
                    (and
                      (eq? (apply + in) (poule-result p add)) 
                      (eq? (apply - in) (poule-result p sub))))
                  n adds subs)))
      (poule-destroy p)
      res)))

(test-group "failure"
  (define (handle-failure worker)
    (let* ((p (poule-create worker 1))
           (j (poule-submit p 'foo))
           (r (handle-exceptions exn
                (cons
                  ((condition-property-accessor 'exn 'location) exn)
                  ((condition-property-accessor 'exn 'message) exn))
                (poule-result p j))))
      (poule-destroy p)
      r))

  (test "signal simple object"
    '(worker . "bar")
    (handle-failure (lambda _ (signal 'bar))))

  (test "signal condition"
    '(worker . "((exn location foo message bar))")
    (handle-failure (lambda _ (signal (condition '(exn location foo message "bar"))))))

  (test "unreadable object"
    '(#f . "(line 1) unreadable object")
    (handle-failure (lambda _ (current-output-port))))

  )

(test-exit)
