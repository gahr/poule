(module (poule)
  
  ;
  ; exports
  ;
  (
   poule?
   poule-create
   poule-submit
   poule-result
   poule-dispose-results
   poule-wait
   poule-destroy
   poule-stats
   poule-trace)

  ;
  ; imports
  ;
  (import
    (scheme)
    (chicken base)
    (chicken condition)
    (chicken file posix)
    (chicken gc)
    (chicken port)
    (chicken process)
    (chicken process signal)
    (chicken process-context posix)
    (chicken string)
    (chicken time)
    (chicken type)
    (mailbox)
    (matchable)
    (srfi-1)
    (srfi-18))

  ;
  ; records
  ;
  (define-record job
                 num    ; job number, incrementally allocated
                 arg    ; argument of the worker function
                 result ; result from the worker function
                 ready? ; is the result ready?
                 )


  (define-record worker
                 pid      ; pid of the child process
                 out      ; parent -> child output port
                 in       ; child -> parent input port
                 job      ; currently assigned job number, or #f if worker is free
                 )

  (define-record poule
                 active?           ; is this poule usable?
                 fn                ; worker function
                 max-workers       ; maximum number of workers to create
                 workers           ; list of workers
                 idle-timeout      ; let workers die after so many idle seconds
                 submission-thread ; thread to submit jobs to workers
                 mbox              ; mailbox for incoming jobs, for the submission thread to pick up
                 jobs              ; list of submitted jobs
                 job-count         ; incremental number to assign each job a unique id 
                 mutex             ; mutuate access between main process and submission thread
                 )

  ;
  ; types
  ;
  (define-type poule (struct poule))

  (: poule-create (('a -> 'b) fixnum #!optional fixnum -> poule))
  (: poule-submit (poule 'a -> fixnum))
  (: poule-result (poule fixnum #!optional boolean -> (or 'b false)))
  (: poule-dispose-results (poule -> undefined))
  (: poule-flush (poule -> undefined))
  (: poule-destroy (poule #!optional boolean -> undefined))
  (: poule-stats (poule -> (list-of (pair symbol undefined))))

  (define poule-trace (make-parameter #f))

  ;
  ; a free worker has a #f job
  ;
  (define worker-free?         (complement worker-job))
  (define (worker-assign! w n) (worker-job-set! w n))
  (define (worker-unassign! w) (worker-job-set! w #f))

  ; debug print
  (define (dp . args)
    (if (poule-trace)
      (apply print
             (current-seconds)
             " [" (current-process-id) "." (thread-name (current-thread)) "] "
             args)))

  ; guard some expressions with the poule mutex
  (define-syntax guarded-p
    (syntax-rules ()
      ((_ body ...)
       (dynamic-wind
         (lambda ()
           (dp "mutex-lock!")
           (mutex-lock! (poule-mutex p)))
         (lambda () body ...)
         (lambda ()
           (dp "mutex-unlock!")
           (mutex-unlock! (poule-mutex p)))))))

  ;
  ; helpers
  ;
  (define (write/flush obj port)
    (write obj port)
    (flush-output port))

  (define (make-backoff initial multiplier)
    (lambda ()
      (dp "Sleeping for " initial)
      (thread-sleep! initial)
      (set! initial (exact->inexact (* initial multiplier)))))

  (define (exn->string e)
    (->string (if (condition? e)
                (condition->list e)
                e)))

  (define (elapsed? last-checkpoint-ms seconds)
    (< (+ last-checkpoint-ms (* 1000 seconds)) (current-process-milliseconds)))


  ;
  ; arguments checkers
  ;
  (define (check-poule p loc)
    (unless (and (poule? p) (poule-active? p))
      (signal (condition `(exn location ,loc message "invalid poule")))))

  (define (check-number n loc)
    (unless (and (fixnum? n) (positive? n))
      (signal (condition `(exn location ,loc message "invalid num")))))

  (define (check-procedure p loc)
    (unless (procedure? p)
      (signal (condition `(exn location ,loc message "invalid procedure")))))

  ; fork a new worker process
  (define (spawn-worker fn idle-timeout)
    (dp "spawn-worker")

    (let-values (((p c) (create-pipe)))
      (define (child)
        (let ((out (open-output-file* c))
              (in  (open-input-file* c))
              (last-idle (current-process-milliseconds)))

          (set-signal-handler!
            signal/alrm
            (lambda (_)
              (when (elapsed? last-idle idle-timeout)
                (dp "idle timeout, exiting...")
                (exit 0))))

          (let loop ()
            (set! last-idle (current-process-milliseconds))
            (set-alarm! idle-timeout) 

            (match (read in)
              (('work x)
               (set-alarm! 0)
               (handle-exceptions exn
                 (write/flush (cons #f (exn->string exn)) out)
                 (write/flush (cons #t (fn x)) out))
               (loop))
              (('exit)
               (dp "got 'exit")
               (write/flush (cons #t #t) out))))))

      (match (process-fork child #t)
        (0 (exit))
        (n
          (dp "spawned worker pid " n)
          (make-worker n (open-output-file* p) (open-input-file* p) #f)))))

  ; gather results from ready workers, kill dead workers, make sure we always
  ; have at least one worker alive
  ; FIXME - this does a bunch of things, it would be best to refactor it a bit
  (define (scan-workers p)
    (dp "scan-workers")

    (define (reap-ready)
      (dp "reap-ready")
      (pair?
        (filter-map
          (lambda (w)
            (and-let* ((n (worker-job w))
                       ((char-ready? (worker-in w)))
                       (j (find (lambda (j) (eq? (job-num j) n)) (poule-jobs p)))
                       (r (handle-exceptions exn
                            (cons #f exn)
                            (read (worker-in w)))))
              ; TODO - convert to condition here, so we don't have
              ; to handle a cons with #t or #f later on?
              (worker-unassign! w)
              (job-ready?-set! j #t)
              (job-arg-set! j #f) ; let it be GC'd
              (job-result-set! j r)))
          (poule-workers p))))

    (define (gc-dead)
      (define (alive? w)
        (let-values (((pid _ _ ) (process-wait (worker-pid w) #t)))
          (zero? pid)))
      (let-values (((alive dead) (partition alive? (poule-workers p))))
        (poule-workers-set!
          p
          (if (pair? alive)
            alive
            (list (spawn-worker (poule-fn p) (poule-idle-timeout p)))))))

    (let ((any-reaped? (reap-ready)))
      (gc-dead)
      any-reaped?))

  ; submission thread
  (define (submission p)
    (dp "submission")

    (define (continue?)
      (thread-specific (current-thread)))

    (let mbox-loop ()
      (dp "mbox-loop")
      (let ((j (mailbox-receive! (poule-mbox p) 1.0 #f)))
        (cond
          ((not (continue?))
           (dp "submit: done"))
          (j
            (let ((backoff (make-backoff 0.1 1.05)))
              (dp "submit: got job " (job-num j))
              (let worker-loop ()
                (cond
                  ((guarded-p
                     (scan-workers p)
                     (and-let* ((w (find worker-free? (poule-workers p))))
                       (dp "submission: assigned job " (job-num j) " to worker " (worker-pid w))
                       (worker-assign! w (job-num j))
                       (write/flush `(work ,(job-arg j)) (worker-out w))
                       #t))
                   (mbox-loop))
                  ((guarded-p
                     (if (> (poule-max-workers p) (length (poule-workers p)))
                       (begin
                         (dp "submit: no worker, spawning a new one")
                         (poule-workers-set!
                           p
                           (cons (spawn-worker (poule-fn p)
                                               (poule-idle-timeout p))
                                 (poule-workers p)))
                         #t)
                       #f))
                   (worker-loop))
                  (else
                    (dp "submit: no worker, backing off")
                    (backoff)
                    (worker-loop))))))
          (else
            (thread-yield!)
            (mbox-loop))))))

  ;
  ; POULE API
  ;
  (define (poule-create fn num #!optional (idle-timeout 15))
    (check-procedure fn 'poule-create)
    (check-number num 'poule-create)

    (let* ((p (make-poule #t
                          fn
                          num
                          (list-tabulate num (lambda (_) (spawn-worker fn idle-timeout)))
                          idle-timeout
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
      p))

  (define (poule-submit p arg)
    (check-poule p 'poule-submit)

    (dp "poule-submit " arg)

    (guarded-p
      (let* ((n (add1 (poule-job-count p)))
             (j (make-job n arg #f #f)))
        (poule-job-count-set! p n)
        (poule-jobs-set! p (cons j (poule-jobs p)))
        (mailbox-send! (poule-mbox p) j)
        n)))

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
      (guarded-p
        (and-let* ((j (find (lambda (j) (eq? (job-num j) num)) (poule-jobs p))))
          (cond
            ((job-ready? j)
             (dp "poule-result " num " is ready")
             (match (job-result j)
               ((and (#t . val) res) res)
               ((#f . val) (signal (if (condition? val)
                                     val
                                     (condition `(exn
                                                   location worker
                                                   message ,val)))))))
            ((null? (poule-workers p))
             (dp "poule-result " num ": all workers have died, bailing out...")
             #f)
            ((scan-workers p)
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
    (guarded-p
      (poule-jobs-set! p (remove job-ready? (poule-jobs p)))))

  (define (poule-wait p)
    (check-poule p 'poule-wait)
    (dp "poule-wait")
    (define backoff (make-backoff 0.1 1.05))
    (let loop ()
      (when
        (guarded-p
          (scan-workers p)
          (and (any (complement job-ready?) (poule-jobs p))
               (not (null? (poule-workers p)))))
        (thread-yield!)
        (backoff)
        (loop))))

  (define (poule-destroy p #!optional (wait? #t))
    (dp "poule-destroy")
    (when (poule-active? p)
      (if wait? (poule-wait p))
      (guarded-p
        (thread-specific-set! (poule-submission-thread p) #f)
        (thread-yield!)
        (let ((w (poule-workers p)))
          (for-each
            (lambda (w)
              (write/flush '(exit) (worker-out w))
              (let-values (((pid succ rc) (process-wait (worker-pid w)))) '()))
            w)
          (poule-active?-set! p #f)))))

  (define (poule-stats p)
    (guarded-p
      (let* ((w (poule-workers p))
             (j (poule-jobs p))
             (idle (length (filter worker-free? w)))
             (busy (- (length w) idle))
             (ready (length (filter job-ready? j)))
             (pending (- (length j) ready)))
        `((submitted-jobs ,(poule-job-count p))
          (pending-jobs ,(mailbox-count (poule-mbox p)))
          (busy-workers ,busy)
          (idle-workers ,idle)
          (ready-results ,ready)
          (pending-results ,pending)))))

  )
