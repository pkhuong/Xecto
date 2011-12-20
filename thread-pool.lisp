(defpackage "WORK-QUEUE"
  (:use "CL" "SB-EXT" "SB-THREAD")
  (:export "TASK" "TASK-P" "BULK-TASK" "BULK-TASK-P"
           "TASK-DESIGNATOR"
           "QUEUE" "MAKE" "P" "ALIVE-P"
           "ENQUEUE" "ENQUEUE-ALL" "STOP"
           "PUSH-SELF" "PUSH-SELF-ALL"
           "PROGRESS-UNTIL"
           "CURRENT-QUEUE" "WORKER-ID" "WORKER-COUNT")
  (:import-from "WORK-STACK"
                "TASK" "TASK-P"
                "BULK-TASK" "BULK-TASK-P"
                "TASK-DESIGNATOR"))

;;; Work-unit queue/stack, with thread-pool
;;;
;;; Normal work queue: created with a fixed number of worker threads,
;;; and a shared FIFO of work units (c.f. work-stack).
;;;
;;; However, each worker also has a work-stack.  This way, tasks can
;;; spawn new tasks recursively, while enjoying temporal locality and
;;; skipping in front of the rest of the queue.
;;;
;;; ENQUEUE/ENQUEUE-ALL insert work units in the queue.
;;;
;;; PUSH-SELF/PUSH-SELF-ALL insert work units in the worker's local
;;; stack, or, if not executed by a worker, punt to ENQUEUE/ENQUEUE-ALL.
;;;
;;; Note that the work-stacks support task-stealing, so pushing to the
;;; local stack does not reduce parallelism.

(in-package "WORK-QUEUE")

(defstruct (queue
            (:constructor %make-queue))
  (locks   (error "foo") :type (simple-array mutex 1)
                         :read-only t)
  (cvar    (make-waitqueue) :type waitqueue
                         :read-only t)
  (nthread (error "foo") :type (and unsigned-byte fixnum)
                         :read-only t)
  (state   (error "foo") :type cons
                         :read-only t)
  (queue   (sb-queue:make-queue) :type sb-queue:queue)
  (stacks  (error "Foo") :type (simple-array work-stack:stack 1)
                         :read-only t)
  (threads (error "Foo") :type (simple-array t 1)
                         :read-only t))

(declaim (inline p))
(defun p (x)
  (queue-p x))

(defun grab-task (queue stacks i)
  (let ((task (sb-queue:dequeue queue)))
    (when task
      (return-from grab-task task)))
  (let* ((n            (length stacks))
         (power-of-two (ash 1 (integer-length (1- n)))))
    (dotimes (j power-of-two)
      (let ((i (logxor i j)))
        (when (>= i n)
          (go skip))
        (let ((task (or (work-stack:steal (aref stacks i))
                        (sb-queue:dequeue queue))))
          (when task
            (return-from grab-task task))))
      skip)))

(defvar *worker-id* nil)
(defvar *worker-hint* 0)
(defvar *current-queue* nil)

(declaim (inline current-queue worker-id worker-count))
(defun current-queue (&optional default)
  (if *current-queue*
      (weak-pointer-value *current-queue*)
      default))

(defun worker-id ()
  *worker-id*)

(defun worker-count (&optional (queue (current-queue)))
  (and queue (queue-nthread queue)))

(defun loop-get-task (state lock cvar queue stacks i
                      &optional max-time)
  (flet ((try ()
           (when (eql (car state) :done)
             (return-from loop-get-task nil))
           (let ((task (grab-task queue stacks i)))
             (when task
               (return-from loop-get-task task)))))
    (declare (inline try))
    (let ((timeout 1e-4)
          (total   0d0)
          (fast    t))
      (declare (single-float timeout)
               (double-float total))
      (loop
       (if fast
           (dotimes (i 128)
             (try)
             (loop repeat (* i 128)
                   do (spin-loop-hint)))
           (try))
        ;; Don't do this at home.
       (setf fast nil)
       (with-mutex (lock)
         (if (condition-wait cvar lock :timeout timeout)
             (setf fast t)
             (grab-mutex lock)))
       (when (and max-time
                  (> (incf total timeout) max-time))
         (return :timeout))
       (setf timeout (min 1.0 (* timeout 1.1)))))))

(declaim (inline %worker-loop))
(defun %worker-loop (weak-queue index hint &optional poll-function wait-time)
  (declare (muffle-conditions code-deletion-note))
  (let* ((wait-time (and poll-function (or wait-time 1)))
         (wqueue    (or (weak-pointer-value weak-queue)
                        (return-from %worker-loop)))
         (i         index)
         (state     (queue-state  wqueue))
         (cvar      (queue-cvar   wqueue))
         (locks     (queue-locks  wqueue))
         (lock      (aref locks i))
         (queue     (queue-queue  wqueue))
         (stacks    (queue-stacks wqueue))
         (stack     (aref stacks i)))
    (labels ((poll ()
               (when poll-function
                 (let ((x (funcall poll-function)))
                   (when x (return-from %worker-loop x)))))
             (work ()
               (loop while (progn
                             (poll)
                             (work-stack:run-one stack))
                     do (when (eq (car state) :done)
                          (return-from %worker-loop)))))
      (declare (inline poll work))
      (work)
      (let ((task (if (and wait-time (zerop wait-time))
                      (grab-task queue stacks i)
                      (loop-get-task state lock cvar
                                     queue stacks i
                                     wait-time))))
        (cond ((not task)
               (poll)
               (return-from %worker-loop))
              (poll-function
               (when (eq task :timeout)
                 (poll)
                 (return-from %worker-loop)))
              (t
               (assert (not (eq task :timeout)))))
        (if (bulk-task-p task)
            (work-stack:push stack task hint)
            (work-stack:execute-task task))
        (work)
        (setf queue nil)))))

(defun %make-worker (wqueue i &optional binding-names binding-compute)
  (declare (type queue wqueue))
  (let* ((state  (queue-state  wqueue))
         (nthread (queue-nthread wqueue))
         (hint   (float (/ i nthread) 1d0))
         (weak-queue (make-weak-pointer wqueue)))
    (make-thread
     (lambda (&aux (*worker-id* i) (*current-queue* weak-queue) (*worker-hint* hint))
       (progv binding-names (mapcar (lambda (x)
                                      (if (functionp x) (funcall x) x))
                                    binding-compute)
         (loop
           (flet ((inner ()
                    (%worker-loop weak-queue i hint)))
             (declare (notinline inner))
             (inner)
             (sb-sys:scrub-control-stack)
             (when (eq (car state) :done)
               (return))))))
     :name (format nil "Work queue worker ~A/~A" i nthread))))

(defun progress-until (condition)
  (let* ((condition (if (functionp condition)
                        condition (fdefinition condition)))
         (i         (worker-id))
         (hint      *worker-hint*)
         (weak-queue *current-queue*)
         (state     (queue-state
                     (or (current-queue)
                         (error "Not in recursive wait?!")))))
    (tagbody
       retry
       (flet ((check ()
                (let ((value (funcall condition)))
                  (when value
                    (return-from progress-until value)))))
         (declare (inline check))
         (%worker-loop weak-queue i hint #'check 0)
         (unless (eql :done (car state))
           (go retry))
         (check)))))

(defun make (nthread &optional constructor &rest arguments)
  (declare (type (and unsigned-byte fixnum) nthread)
           (dynamic-extent arguments))
  (let* ((threads (make-array nthread))
         (default-bindings (getf arguments :bindings))
         (arguments (loop for (key value) on arguments by #'cddr
                          unless (eql key :bindings)
                            nconc (list key value)))
         (wqueue  (apply (or constructor #'%make-queue)
                         :locks   (map-into (make-array nthread) #'make-mutex)
                         :cvar    (make-waitqueue)
                         :nthread nthread
                         :state   (list :running)
                         :queue   (sb-queue:make-queue)
                         :stacks  (map-into (make-array nthread) #'work-stack:make)
                         :threads threads
                         arguments)))
    (finalize wqueue (let ((cvar  (queue-cvar  wqueue))
                           (state (queue-state wqueue)))
                      (lambda ()
                        (setf (car state) :done)
                        (condition-broadcast cvar))))
    (let ((binding-names (mapcar #'car default-bindings))
          (binding-values (mapcar #'cdr default-bindings)))
      (dotimes (i nthread wqueue)
        (setf (aref threads i)
              (%make-worker wqueue i binding-names binding-values))))))

(defun stop (queue)
  (declare (type queue queue))
  (setf (car (queue-state queue)) :done)
  (condition-broadcast (queue-cvar queue))
  nil)

(defun alive-p (queue)
  (declare (type queue queue))
  (eql (car (queue-state queue)) :running))

(defun enqueue (task &optional (queue (current-queue)))
  (declare (type task-designator task)
           (type queue queue))
  (assert (alive-p queue))
  (sb-queue:enqueue task (queue-queue queue))
  (condition-broadcast (queue-cvar queue))
  nil)

(defun enqueue-all (tasks &optional (queue (current-queue)))
  (declare (type queue queue))
  (assert (alive-p queue))
  (let ((queue (queue-queue queue)))
    (map nil (lambda (task)
               (sb-queue:enqueue task queue))
         tasks))
  (condition-broadcast (queue-cvar queue))
  nil)

(defun push-self (task &optional (queue (current-queue)))
  (declare (type queue queue)
           (type task-designator task))
  (assert (alive-p queue))
  (let ((id *worker-id*))
    (cond (id
           (assert (eql (aref (queue-threads queue) id)
                        *current-thread*))
           (work-stack:push (aref (queue-stacks queue) id) task *worker-hint*))
          (t
           (enqueue task queue)))))

(defun push-self-all (tasks &optional (queue (current-queue)))
  (declare (type queue queue))
  (assert (alive-p queue))
  (let ((id *worker-id*))
    (cond (id
           (assert (eql (aref (queue-threads queue) id)
                        *current-thread*))
           (work-stack:push-all (aref (queue-stacks queue) id) tasks *worker-hint*))
          (t
           (enqueue-all tasks queue)))))
