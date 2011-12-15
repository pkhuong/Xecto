(defpackage "WORK-QUEUE"
  (:use "CL" "SB-EXT" "SB-THREAD")
  (:export "MAKE" "P" "QUEUE" "ALIVE-P"
           "TASK" "TASK-HASH" "TASK-FUN" "HASH" "FUN"
           "ENQUEUE" "ENQUEUE-ALL" "STOP"
           "PUSH-SELF" "PUSH-SELF-ALL")
  (:nicknames "WQ"))

(in-package "WORK-QUEUE")

(defglobal **hash-state** (make-random-state t))
(defglobal **hash-lock**  (make-mutex :name "WORK-QUEUE HASH-LOCK"))

(defun get-task-hash (task)
  (or (task-hash task)
      (with-mutex (**hash-lock**)
        (random (1+ most-positive-fixnum) **hash-state**))))

(defstruct (task
            (:constructor nil))
  (hash nil :type (or null (and unsigned-byte fixnum))
            :read-only t)
  (fun  nil :type (or symbol function)
            :read-only t)
  (%executed nil))

(defstruct stack
  (lock   (make-mutex) :type mutex
                       :read-only t)
  (data   (make-array 32 :fill-pointer 0 :adjustable t)
                       :type (and (not simple-vector)
                                  vector)
                       :read-only t))

(defun stack-push (stack x)
  (with-mutex ((stack-lock stack))
    (vector-push-extend x (stack-data stack))))

(defun stack-push-all (stack values)
  (with-mutex ((stack-lock stack))
    (let ((data (stack-data stack)))
      (map nil (lambda (x)
                 (vector-push-extend x data))
           values))))

(defun stack-pop (stack)
  (with-mutex ((stack-lock stack))
    (let* ((data  (stack-data stack))
           (index (position nil data :from-end t :test-not #'eql)))
      (prog1 (and index
                  (shiftf (aref data index) nil))
        (setf (fill-pointer data) (or index 0))))))

(defun stack-steal (stack)
  (with-mutex ((stack-lock stack))
    ;; todo: hash for affinity
    (let* ((data  (stack-data stack))
           (index (position nil data :test-not #'eql)))
      (and index
           (shiftf (aref data index) nil)))))

(defstruct (queue
            (:constructor %make-queue
                (nthread state queues stacks threads)))
  (lock    (make-mutex)  :type mutex
                         :read-only t)
  (cvar    (make-waitqueue) :type waitqueue
                         :read-only t)
  (nthread (error "foo") :type (and unsigned-byte fixnum)
                         :read-only t)
  (state   (error "foo") :type cons
                         :read-only t)
  (queues  (error "Foo") :type (simple-array sb-queue:queue 1)
                         :read-only t)
  (stacks  (error "Foo") :type (simple-array stack 1)
                         :read-only t)
  (threads (error "Foo") :type (simple-array t 1)
                         :read-only t))

(declaim (inline p))
(defun p (x)
  (queue-p x))

(defun grab-task (queues stacks i)
  (let ((n (length queues)))
    (dotimes (j n)
      (let* ((i    (mod (+ i j) n))
             (task (sb-queue:dequeue (aref queues i))))
        (when task
          (return-from grab-task task)))))
  (let ((n (length stacks)))
    (dotimes (j n)
      (let* ((i    (mod (+ i j) n))
             (task (stack-steal (aref stacks i))))
        (when task
          (return-from grab-task task))))))

(defvar *worker-id* nil)

(defun %make-worker (queue i)
  (let* ((lock   (queue-lock queue))
         (cvar   (queue-cvar queue))
         (state  (queue-state queue))
         (queues (queue-queues queue))
         (stacks (queue-stacks queue))
         (stack  (aref stacks i)))
    (make-thread
     (lambda (&aux (*worker-id* i))
       (loop
        (let ((task
                (with-mutex (lock)
                  (loop
                    (when (eql (car state) :done)
                      (return nil))
                    (let ((task (grab-task queues stacks i)))
                      (when task
                        (return task)))
                    (condition-wait cvar lock)))))
          (unless task
            (return))
          (stack-push stack task)
          (loop for task = (stack-pop stack)
                while task
                do (assert (null (compare-and-swap (task-%executed task) nil t)))
                   (funcall (task-fun task) task)))))
     :name "Work queue worker")))

(defun make (nthread)
  (declare (type (and unsigned-byte fixnum) nthread))
  (let* ((threads (make-array nthread))
         (queue   (%make-queue nthread
                               (list :running)
                               (map-into (make-array nthread) #'sb-queue:make-queue)
                               (map-into (make-array nthread) #'make-stack)
                               threads)))
    (finalize queue (let ((lock  (queue-lock queue))
                          (cvar  (queue-cvar queue))
                          (state (queue-state queue)))
                      (lambda ()
                        (with-mutex (lock)
                          (setf (car state) :done)
                          (condition-broadcast cvar)))))
    (dotimes (i nthread queue)
      (setf (aref threads i)
            (%make-worker queue i)))))

(defun stop (queue)
  (declare (type queue queue))
  (with-mutex ((queue-lock queue))
    (setf (car (queue-state queue)) :done)
    (condition-broadcast (queue-cvar queue)))
  nil)

(defun alive-p (queue)
  (declare (type queue queue))
  (eql (car (queue-state queue)) :running))

(defun enqueue (queue task)
  (declare (type queue queue)
           (type task  task))
  (with-mutex ((queue-lock queue))
    (assert (alive-p queue))
    (let ((index (mod (get-task-hash task) (queue-nthread queue))))
      (sb-queue:enqueue task (aref (queue-queues queue) index)))
    (condition-broadcast (queue-cvar queue)))
  nil)

(defun enqueue-all (queue tasks)
  (declare (type queue queue))
  (with-mutex ((queue-lock queue))
    (assert (alive-p queue))
    (let ((nthread (queue-nthread queue))
          (queues  (queue-queues  queue)))
      (map nil (lambda (task)
                 (let ((index (mod (get-task-hash task) nthread)))
                   (sb-queue:enqueue task (aref queues index))))
           tasks))
    (condition-broadcast (queue-cvar queue)))
  nil)

(defun push-self (queue task)
  (declare (type queue queue)
           (type task  task))
  (assert (alive-p queue))
  (let ((id *worker-id*))
    (cond (id
           (assert (eql (aref (queue-threads queue) id)
                        *current-thread*))
           (stack-push (aref (queue-stacks queue) id) task))
          (t
           (enqueue queue task)))))

(defun push-self-all (queue tasks)
  (declare (type queue queue))
  (assert (alive-p queue))
  (let ((id *worker-id*))
    (cond (id
           (assert (eql (aref (queue-threads queue) id)
                        *current-thread*))
           (let ((stack (aref (queue-stacks queue) id)))
             (stack-push-all stack tasks)))
          (t
           (enqueue-all queue tasks)))))
