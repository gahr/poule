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
    (datatype)
    (mailbox)
    (matchable)
    (srfi-1)
    (srfi-18)
    (typed-records))

  (define-syntax record
    (syntax-rules ()
      ((_ name body ...)
       (begin
         (defstruct name body ...)
         (define-type name (struct name))))))

  ;
  ; datatypes
  ;
  (define-datatype result result?
                   (result-value (val (constantly #t)))
                   (result-error (err (constantly #t)))) 
  (define-type result (struct result))

  ;
  ; records
  ;
  (record job
          (num         : fixnum)            ; job number, incrementally allocated
          (arg         : *)                 ; argument of the worker function
          ((result #f) : (or result false)) ; result from the worker function
          ((ready? #f) : boolean)           ; is the result ready?
          )


  (record worker
          (pid      : fixnum)            ; pid of the child process
          (out      : output-port)       ; parent -> child output port
          (in       : input-port)        ; child -> parent input port
          ((job #f) : (or fixnum false)) ; currently assigned job number, or #f if worker is free
          )

  (record poule
          ((active? #t)          : boolean)           ; is this poule usable?
          (fn                    : procedure)         ; worker function
          (max-workers           : fixnum)            ; maximum number of workers to create
          (workers               : (list-of worker))  ; list of workers
          (idle-timeout          : fixnum)            ; let workers die after so many idle seconds
          (submission-thread     : thread)            ; thread to submit jobs to workers
          ((mbox (make-mailbox)) : (struct mailbox))  ; mailbox for incoming jobs, for the submission thread to pick up
          ((jobs '())            : (list-of job))     ; list of submitted jobs
          ((job-count 0)         : fixnum)            ; incremental number to assign each job a unique id
          ((mutex (make-mutex))  : (struct mutex))    ; mutuate access between main process and submission thread
          )

  (: poule-create          (('a -> 'b) fixnum #!optional fixnum -> poule))
  (: poule-submit          (poule 'a -> fixnum))
  (: poule-result          (poule fixnum #!optional boolean -> (or 'b false)))
  (: poule-dispose-results (poule -> undefined))
  (: poule-flush           (poule -> undefined))
  (: poule-destroy         (poule #!optional boolean -> undefined))
  (: poule-stats           (poule -> (list-of (pair symbol undefined))))

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

  (define (next-job-number p)
    (let ((n (add1 (poule-job-count p))))
      (poule-job-count-set! p n)
      n))

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

  (define (check-boolean b loc)
    (unless (boolean? b)
      (signal (condition `(exn location ,loc message "invalid boolean")))))

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
        (0
         (exit))
        (n
          (dp "spawned worker pid " n)
          (make-worker pid: n
                       out: (open-output-file* p)
                       in:  (open-input-file* p))))))

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
                            (result-error exn)
                            (match (read (worker-in w))
                              ((#t . val) (result-value val))
                              ((#f . err) (result-error err))))))
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
                (guarded-p (scan-workers p))
                (cond
                  ((guarded-p
                     (and-let* ((w (find worker-free? (poule-workers p))))
                       (dp "submit: assigned job " (job-num j) " to worker " (worker-pid w))
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

    (letrec* ((t (make-thread (cut submission p)))
              (p (make-poule fn:                fn
                             max-workers:       num
                             workers:           (list-tabulate num (lambda (_) (spawn-worker fn idle-timeout)))
                             idle-timeout:      idle-timeout
                             submission-thread: t)))
      (thread-specific-set! t #t)
      (set-finalizer! p poule-destroy)
      (thread-start! t)
      p))

  (define (poule-submit p arg)
    (check-poule p 'poule-submit)

    (dp "poule-submit " arg)

    (guarded-p
      (let* ((n (next-job-number p))
             (j (make-job num: n arg: arg)))
        (poule-job-count-set! p n)
        (poule-jobs-set! p (cons j (poule-jobs p)))
        (mailbox-send! (poule-mbox p) j)
        n)))

  (define (poule-result p num #!optional (wait? #t))
    (check-poule p 'poule-result)
    (check-number num 'poule-result)
    (check-boolean wait? 'poule-result)

    (define backoff (make-backoff 0.1 1.05))

    (define-datatype try-result
                     (try-done (val (constantly #t)))
                     (try-retry-now)
                     (try-retry-later))
    (define (try)
      (guarded-p
        (let ((j (find (lambda (j) (eq? (job-num j) num)) (poule-jobs p))))
          (if (or (not j) (null? (poule-workers p)))
            (try-done #f)
            (cond
              ((job-ready? j)
               (dp "poule-result " num " is ready")
               (cases result (job-result j)
                 (result-value (v) (try-done v))
                 (result-error (e) (signal (if (condition? e)
                                             e
                                             (condition `(exn
                                                           location worker
                                                           message ,e)))))))
              ((scan-workers p)
               (dp "poule-result " num ": some worker is done, trying again...")
               (try-retry-now))
              (wait?
                (dp "poule-result " num ": worker is not done, backing off...")
                (try-retry-later))
              (else
                (dp "poule-result " num ": worker is not done, returning")
                (try-done #f)))))))

    (let loop ()
      (dp "looping")
      (let ((t (try)))
        (cases try-result t
          (try-done (val) val)
          (try-retry-now () (loop))
          (try-retry-later () (backoff) (loop))))))

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
