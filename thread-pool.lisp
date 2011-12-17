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
            (:constructor %make-queue
                (nthread state queue stacks threads)))
  (lock    (make-mutex)  :type mutex
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

(defun current-queue ()
  (the queue *current-queue*))

(defun %make-worker (wqueue i &optional binding-names binding-compute)
  (let* ((lock   (queue-lock   wqueue))
         (cvar   (queue-cvar   wqueue))
         (state  (queue-state  wqueue))
         (queue  (queue-queue  wqueue))
         (stacks (queue-stacks wqueue))
         (nthread (queue-nthread wqueue))
         (stack  (aref stacks i))
         (hint   (float (/ i nthread) 1d0)))
    (make-thread
     (lambda (&aux (*worker-id* i) (*current-queue* wqueue) (*worker-hint* hint))
       (progv binding-names (mapcar (lambda (x)
                                      (if (functionp x) (funcall x) x))
                                    binding-compute)
         (loop named outer do
           (let* ((timeout 1e-4)
                  (task
                    (with-mutex (lock)
                      (loop
                        (when (eql (car state) :done)
                          (return-from outer))
                        (let ((task (grab-task queue stacks i)))
                          (when task
                            (return task)))
                        (unless (condition-wait cvar lock :timeout timeout)
                          (grab-mutex lock))
                        (when (< timeout 1e0)
                          (setf timeout (* timeout 2)))))))
             (declare (type single-float timeout))
             (if (bulk-task-p task)
                 (work-stack:push stack task hint)
                 (work-stack:execute-task task))
             (loop while (work-stack:run-one stack))))))
     :name (format nil "Work queue worker ~A/~A" i nthread))))

(defun make (nthread &optional constructor &rest arguments)
  (declare (type (and unsigned-byte fixnum) nthread)
           (dynamic-extent arguments))
  (let* ((state   (list :running))
         (queue   (sb-queue:make-queue))
         (stacks  (map-into (make-array nthread) #'work-stack:make))
         (threads (make-array nthread))
         (default-bindings (getf arguments :bindings))
         (arguments (loop for (key value) on arguments by #'cddr
                          unless (eql key :bindings)
                            nconc (list key value)))
         (wqueue  (if constructor
                      (apply constructor
                             :lock    (make-mutex)
                             :cvar    (make-waitqueue)
                             :nthread nthread
                             :state   state
                             :queue   queue
                             :stacks  stacks
                             :threads threads
                             arguments)
                      (%make-queue nthread
                                   state
                                   queue
                                   stacks
                                   threads))))
    (finalize wqueue (let ((lock  (queue-lock  wqueue))
                           (cvar  (queue-cvar  wqueue))
                           (state (queue-state wqueue)))
                      (lambda ()
                        (with-mutex (lock)
                          (setf (car state) :done)
                          (condition-broadcast cvar)))))
    (let ((binding-names (mapcar #'car default-bindings))
          (binding-values (mapcar #'cdr default-bindings)))
      (dotimes (i nthread wqueue)
        (setf (aref threads i)
              (%make-worker wqueue i binding-names binding-values))))))

(defun stop (queue)
  (declare (type queue queue))
  (with-mutex ((queue-lock queue))
    (setf (car (queue-state queue)) :done)
    (condition-broadcast (queue-cvar queue)))
  nil)

(defun alive-p (queue)
  (declare (type queue queue))
  (eql (car (queue-state queue)) :running))

(defun enqueue (task &optional (queue *current-queue*))
  (declare (type task-designator task)
           (type queue queue))
  (with-mutex ((queue-lock queue))
    (assert (alive-p queue))
    (sb-queue:enqueue task (queue-queue queue))
    (condition-broadcast (queue-cvar queue)))
  nil)

(defun enqueue-all (tasks &optional (queue *current-queue*))
  (declare (type queue queue))
  (with-mutex ((queue-lock queue))
    (assert (alive-p queue))
    (let ((queue (queue-queue queue)))
      (map nil (lambda (task)
                 (sb-queue:enqueue task queue))
           tasks))
    (condition-broadcast (queue-cvar queue)))
  nil)

(defun push-self (task &optional (queue *current-queue*))
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

(defun push-self-all (tasks &optional (queue *current-queue*))
  (declare (type queue queue))
  (assert (alive-p queue))
  (let ((id *worker-id*))
    (cond (id
           (assert (eql (aref (queue-threads queue) id)
                        *current-thread*))
           (work-stack:push-all (aref (queue-stacks queue) id) tasks *worker-hint*))
          (t
           (enqueue-all tasks queue)))))
