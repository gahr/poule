(module (poule)
  
  ; exports
  
  (
   poule?
   poule-create
   poule-submit
   poule-result
   poule-dispose-results
   poule-wait
   poule-destroy
   poule-trace)

  ; imports
  (import
    (scheme)
    (chicken base)
    (chicken condition)
    (chicken file posix)
    (chicken gc)
    (chicken port)
    (chicken process)
    (chicken process-context posix)
    (chicken string)
    (chicken type)
    (mailbox)
    (matchable)
    (srfi-1)
    (srfi-18))

  ; types
  (define-record job
                 num      ; job number, incrementally allocated
                 status   ; 'pending or 'available
                 arg      ; argument for the worker function
                 result   ; result of the worker function
                 )


  (define-record worker
                 pid      ; pid of the child process
                 out      ; parent -> child output port
                 in       ; child -> parent input port
                 job-num  ; currently assigned job number, or #f if worker is free
                 )

  (define-record poule
                 active?
                 fn
                 workers
                 submission-thread
                 queue
                 jobs
                 last-job-num
                 mutex
                 )

  (define poule-trace (make-parameter #f))

  (define (dp . args)
    (if (poule-trace)
      (apply print
             "[" (current-process-id) "." (thread-name (current-thread)) "] "
             args)))

  (define-syntax with-synchronized
    (syntax-rules ()
      ((_ p body ...)
       (dynamic-wind
         (lambda ()
           (dp "mutex-lock!")
           (mutex-lock! (poule-mutex p)))
         (lambda () body ...)
         (lambda ()
           (dp "mutex-unlock!")
           (mutex-unlock! (poule-mutex p)))))))

  (define (write/flush obj port)
    (write obj port)
    (flush-output port))

  (define (make-backoff initial multiplier)
    (lambda ()
      (dp "Sleeping for " initial)
      (thread-sleep! initial)
      (set! initial (exact->inexact (* initial multiplier)))))

  (define (check-poule p loc)
    (unless (and (poule? p) (poule-active? p))
      (signal (condition `(exn location ,loc message "invalid poule")))))

  (define (check-number n loc)
    (unless (and (fixnum? n) (positive? n))
      (signal (condition `(exn location ,loc message "invalid num")))))

  (define (check-procedure p loc)
    (unless (procedure? p)
      (signal (condition `(exn location ,loc message "invalid procedure")))))

  (define (scan-workers p)
    (dp "scan-workers")
    (filter-map
      (lambda (w)
        (and-let* ((n (worker-job-num w))
                   ((char-ready? (worker-in w)))
                   (j (find (lambda (j) (eq? (job-num j) n)) (poule-jobs p)))
                   (r (handle-exceptions exn
                        (cons #f exn)
                        (read (worker-in w)))))
          ; TODO - convert to condition here, so we don't have
          ; to handle a cons with #t or #f later on?
          (worker-job-num-set! w #f)
          (job-status-set! j 'available)
          (job-arg-set! j #f) ; let it be GC'd
          (job-result-set! j r)
          w))
      (poule-workers p)))

  (define (exn->string e)
    (->string (if (condition? e)
                (condition->list e)
                e)))

  (define (spawn-worker fn)
    (let-values (((p c) (create-pipe)))
      (define (child)
        (let ((out (open-output-file* c))
              (in  (open-input-file* c)))
          (let loop ()
            (match (read in)
              (('work x)
               (handle-exceptions exn
                 (write/flush (cons #f (exn->string exn)) out)
                 (write/flush (cons #t (fn x)) out))
               (loop))
              (('exit)
               (dp "got 'exit")
               (write/flush (cons #t #t) out))))))

      (match (process-fork child)
        (0
         (dp "fork returned 0")
         (exit))
        (n
          (dp "fork returned " n)
          (make-worker n (open-output-file* p) (open-input-file* p) #f)))))

  (define (submission p)
    (define (find-free-worker)
      (or
        (and-let* ((w (find (complement worker-job-num) (poule-workers p))))
          (dp "found free worker " (worker-pid w))
          w)
        (and-let* ((w (scan-workers p))
                   ((not (null? w))))
          (dp "found worker done " (worker-pid (car w)))
          (car w))))

    (define (continue?)
      (thread-specific (current-thread)))

    (let mbox-loop ()
      (let ((j (mailbox-receive! (poule-queue p) 0.1 #f))
            (backoff (make-backoff 0.1 1.05)))
        (cond
          ((not (continue?))
           (dp "submit: done"))
          (j
            (let worker-loop ()
              (cond
                ((with-synchronized
                   p
                   (and-let* ((w (find-free-worker)))
                     (dp "submission: job " (job-num j) " to worker " (worker-pid w))
                     (worker-job-num-set! w (job-num j))
                     (write/flush `(work ,(job-arg j)) (worker-out w))
                     #t))
                 (mbox-loop))
                (else
                  (dp "submit: no worker, backing off")
                  (backoff)
                  (worker-loop)))))
          (else
            (mbox-loop))))))

  (define (poule-create fn num)
    (check-procedure fn 'poule-create)
    (check-number num 'poule-create)

    (let* ((p (make-poule #t
                          fn
                          (list-tabulate num (lambda (n) (spawn-worker fn)))
                          #f ; created later
                          (make-mailbox)
                          '()
                          0
                          (make-mutex)))
           (t (make-thread (cut submission p))))
      (thread-specific-set! t #t)
      (poule-submission-thread-set! p t)
      (set-finalizer! p poule-destroy)
      (thread-start! t)
      p) 
    )

  (define (poule-submit p arg)
    (check-poule p 'poule-submit)

    (dp "poule-submit " arg)

    (with-synchronized
      p
      (let ((j (make-job (add1 (poule-last-job-num p))
                         'pending
                         arg
                         #f)))
        (poule-last-job-num-set! p (job-num j))
        (poule-jobs-set! p (cons j (poule-jobs p)))
        (mailbox-send! (poule-queue p) j)
        (job-num j))))

  (define (poule-result p num #!optional (wait? #t))
    (check-poule p 'poule-result)
    (check-number num 'poule-result)

    (define backoff (make-backoff 0.1 1.05))

    ; try to get a result, return
    ; - #f         -> result number is invalid
    ; - (#t . val) -> result found -> return val
    ; - (#f . #t ) -> result not found, but some worker is done ->  retry immediately
    ; - (#f . #f ) -> result not found, no worker is done -> retry after sleeping
    (define (try)
      (with-synchronized
        p
        (and-let* ((j (find (lambda (j) (eq? (job-num j) num)) (poule-jobs p))))
          (cond
            ((eq? 'available (job-status j))
             (dp "poule-result " num ": is ready")
             (match (job-result j)
               ((#t . val) (cons #t val))
               ((#f . val) (signal (if (condition? val)
                                     val
                                     (condition `(exn
                                                   location worker
                                                   message ,val)))))))
            ((not (null? (scan-workers p)))
             (dp "poule-result " num ": some worker is done, trying again...")
             (cons #f #t))
            (wait?
              (dp "poule-result " num ": worker is not done, backing off...")
              (cons #f #f))
            (else (cons #t #f))))))

    (let loop ()
      (match (try)
        ((#t . x ) x)   
        ((#f . #f) (backoff) (loop))
        ((#f . #t) (loop))
        (#f        #f))))

  (define (poule-dispose-results p)
    (check-poule p 'poule-dispose-results)
    (with-synchronized
      p
      (poule-jobs-set! p
                       (remove
                         (lambda (j) (eq? 'available (job-status j)))
                         (poule-jobs p)))))

  (define (poule-wait p)
    (check-poule p 'poule-wait)
    (dp "poule-wait")
    (define backoff (make-backoff 0.1 1.05))
    (let loop ()
      (when
        (with-synchronized
          p
          (scan-workers p)
          (any (lambda (j) (eq? 'pending (job-status j))) (poule-jobs p)))
        (thread-yield!)
        (backoff)
        (loop))))

  (define (poule-destroy p #!optional (wait? #t))
    (dp "poule-destroy")
    (when (poule-active? p)
      (if wait? (poule-wait p))
      (with-synchronized
        p
        (thread-specific-set! (poule-submission-thread p) #f)
        (thread-yield!)
        (let ((w (poule-workers p)))
          (for-each
            (lambda (w)
              (write/flush '(exit) (worker-out w))
              (let-values (((pid succ rc) (process-wait (worker-pid w)))) '()))
            w)
          (poule-active?-set! p #f)))))
  )
