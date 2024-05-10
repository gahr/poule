(import
  (poule)
  (chicken base)
  (chicken process-context posix)
  (chicken process signal)
  (chicken random)
  (chicken time)
  (define-options)
  (srfi-1) 
  (srfi-18))

(define-options
  (simulate)
  `(
    (duration 
      "Duration of the simulation, in seconds" 
      (default 600) 
      (value 
        (required ms) 
        (predicated ,integer?) 
        (transformer ,string->number)))
    (main-sleep-min 
      "Minimum sleep time for the producer, in milliseconds" 
      (default 20) 
      (value 
        (required ms) 
        (predicated ,integer?) 
        (transformer ,string->number)))
    (main-sleep-max 
      "Maximum sleep time for the producer, in milliseconds" 
      (default 100) 
      (value 
        (required ms) 
        (predicated ,integer?) 
        (transformer ,string->number)))
    (work-time-min 
      "Minimum job duration, in milliseconds" 
      (default 200) 
      (value 
        (required ms) 
        (predicated ,integer?) 
        (transformer ,string->number)) )
    (work-time-max 
      "Maximum job duration, in milliseconds" 
      (default 500) 
      (value 
        (required ms) 
        (predicated ,integer?) 
        (transformer ,string->number)))
    (wait-all-prob 
      "Probability to wait for all jobs, in percentage" 
      (default 10) 
      (value 
        (required pct) 
        (predicated ,integer?) 
        (transformer ,string->number)))
    (wait-half-prob 
      "Probability to wait for half the jobs, in percentage" 
      (default 40) 
      (value 
        (required pct) 
        (predicated ,integer?) 
        (transformer ,string->number)))
    (print-stats-every 
      "Print statistics every so many jobs" 
      (default 100) 
      (value 
        (required jobs) 
        (predicated ,integer?) 
        (transformer ,string->number)))
    (trace
      "Trace log" 
      (default #f))))

(poule-trace trace)

(define (sleep/random lo hi)
  (let ((s (exact->inexact (/ (+ lo (pseudo-random-integer (- hi lo))) 1000))))
    (thread-sleep! s)))

(define (sleep/random/main) (sleep/random main-sleep-min main-sleep-max))
(define (sleep/random/work) (sleep/random work-time-min work-time-max))

(define (dp . args)
  (apply print
         (current-seconds)
         " [" (current-process-id) "." (thread-name (current-thread)) "] "
         args)
  (flush-output))

(let* ((poule (poule-create (lambda (item) (dp "got job " item) (sleep/random/work) (dp "done") item) 10))
       (wait-result (lambda (j) (dp "job " j " -> " (poule-result poule j))))
       (stop? #f))

  (set-signal-handler! signal/alrm (lambda (_) (set! stop? #t)))
  (set-alarm! duration)

  (let loop ((jobs '()) (i 0))
    (when stop?
      (dp "wrapping up...")
      (poule-destroy poule)
      (exit 0))

    (when (eq? 0 (modulo i print-stats-every))
      (dp "stats: " (poule-stats poule)))

    (sleep/random/main)
    (let* ((num (pseudo-random-integer 1000))
           (job (poule-submit poule num)))
      (cond
        ((< num (* 10 wait-all-prob))
         (dp "waiting for all the jobs...")
         (for-each wait-result (cons job jobs))
         (poule-dispose-results poule)
         (loop '() (add1 i)))
        ((< num (* 10 wait-half-prob))
         (dp "waiting for half the jobs...")
         (let-values (((wait keep) (split-at jobs (floor (/ (length jobs) 2)))))
           (for-each wait-result wait)
           (loop (cons job keep) (add1 i))))
        (else
          (loop (cons job jobs) (add1 i)))))))
