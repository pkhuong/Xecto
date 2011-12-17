(defpackage "WORK-QUEUE"
  (:use "CL" "SB-EXT" "SB-THREAD")
  (:export "TASK" "TASK-P" "BULK-TASK" "BULK-TASK-P"
           "TASK-DESIGNATOR"
           "QUEUE" "MAKE" "P" "ALIVE-P"
           "ENQUEUE" "ENQUEUE-ALL" "STOP"
           "PUSH-SELF" "PUSH-SELF-ALL"
           "CURRENT-QUEUE")
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
  (let ((n (length stacks)))
    (dotimes (j n)
      (let* ((i    (mod (+ i j) n))
             (task (work-stack:steal (aref stacks i))))
        (when task
          (return-from grab-task task))))))

(defvar *worker-id* nil)
(defvar *worker-hint* 0)
(defvar *current-queue* nil)

(declaim (inline current-queue))
(defun current-queue ()
  (and *current-queue*
       (weak-pointer-value *current-queue*)))

(defun loop-get-task (state lock cvar queue stacks i)
  (flet ((try ()
           (when (eql (car state) :done)
             (return-from loop-get-task nil))
           (let ((task (grab-task queue stacks i)))
             (when task
               (return-from loop-get-task task)))))
    (declare (inline try))
    (dotimes (i 256)
      (try)
      (loop repeat (* i 128)
            do (spin-loop-hint)))
    (with-mutex (lock)
      (let ((timeout 1e-5))
        (declare (single-float timeout))
        (loop
          (try)
          (unless (condition-wait cvar lock :timeout timeout)
            (grab-mutex lock))
          (setf timeout (min 1.0 (* timeout 1.1))))))))

(defun %make-worker (wqueue i &optional binding-names binding-compute)
  (let* ((locks  (queue-locks  wqueue))
         (cvar   (queue-cvar   wqueue))
         (state  (queue-state  wqueue))
         (queue  (queue-queue  wqueue))
         (stacks (queue-stacks wqueue))
         (nthread (queue-nthread wqueue))
         (stack  (aref stacks i))
         (hint   (float (/ i nthread) 1d0))
         (weak-queue (make-weak-pointer wqueue)))
    (make-thread
     (lambda (&aux (*worker-id* i) (*current-queue* weak-queue) (*worker-hint* hint))
       (progv binding-names (mapcar (lambda (x)
                                      (if (functionp x) (funcall x) x))
                                    binding-compute)
         (loop named outer do
           (let ((task  (loop-get-task state (aref locks i) cvar
                                       queue stacks i))
                 (queue (weak-pointer-value weak-queue)))
             (unless (and task queue)
               (return-from outer))
             (if (bulk-task-p task)
                 (work-stack:push stack task hint)
                 (work-stack:execute-task task))
             (loop while (work-stack:run-one stack))
             (setf queue nil)))))
     :name (format nil "Work queue worker ~A/~A" i nthread))))

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
