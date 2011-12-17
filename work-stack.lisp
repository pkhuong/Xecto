(defpackage "WORK-STACK"
  (:use "CL" "SB-EXT" "SB-THREAD")
  (:shadow cl:push)
  (:export "TASK" "TASK-P" "BULK-TASK" "BULK-TASK-P"
           "TASK-DESIGNATOR"
           "EXECUTE-TASK"
           "STACK" "MAKE" "P"
           "PUSH" "PUSH-ALL" "STEAL" "RUN-ONE"))

;;; Work-unit stack
;;;
;;; Normal task-stealing stack, with special support for tasks composed
;;; of subtasks.
;;;
;;; A task designator is either a function designator, a task, or a
;;; bulk-task.
;;;
;;; A function designator is called, and a task's fun is called with the
;;; task as its only argument.
;;;
;;; When only those are used, the work stack is a normal stack of task
;;; units, with PUSH to insert a new task (PUSH-ALL to insert a sequence
;;; of tasks), STEAL to get one task from the bottom of the stack, and
;;; RUN-ONE to execute and pop the topmost task.
;;;
;;; Bulk-task objects represent a set of subtasks to be executed, and
;;; a sequence of operations to perform once all the subtasks have been
;;; completed.
;;;
;;; Task stealing of bulk tasks is special: bulk tasks have multiple
;;; owners, so bulk tasks aren't stolen as much as forcibly shared.  All
;;; the workers that share a bulk task cooperate to complete the subtasks;
;;; the last worker to finish executing a subtask then executes the
;;; cleanups.
;;;
;;; Subtasks and cleanups are functions that are called with the
;;; subtask as their one argument.
;;;
;;; Cooperating threads avoid hammering the same subtasks by
;;; beginning/resuming their search for remaining subtasks from different
;;; indices: PUSH/PUSH-ALL take an optional argument to determine the
;;; fraction of the subtask vector from which to initialise the thread's
;;; search (defaults to 0).  Incidentally, this is also useful for
;;; locality, when the subtasks are sorted right.

(in-package "WORK-STACK")

(defstruct (task
            (:constructor nil))
  (function (error "Missing arg") :type (or symbol function)))

(defstruct (bulk-task
            (:constructor nil))
  ;; count waiting to be executed, initially (length subtasks)
  (waiting   (error "Missing arg") :type word)
  ;; count not done yet, initially (length subtasks)
  (remaining (error "Missing arg") :type word)
  (subtasks  (error "Missing arg") :type (simple-array (or symbol function) 1))
  (cleanup   nil                   :type (or list symbol function)))

(deftype task-designator ()
  `(or symbol function task bulk-task))

(defun execute-task (task)
  (etypecase task
    ((or symbol function)
     (funcall task))
    (task
     (prog1 (funcall (task-function task) task)
       (setf (task-function task) nil)))))

(defconstant +stacklet-size+ 128)

(declaim (inline split-index))
(defun split-index (index)
  (multiple-value-bind (major minor)
      (truncate index +stacklet-size+)
    (cond ((plusp minor)
           (values major minor))
          ((zerop major)
           (values 0 0))
          (t
           (values (1- major) +stacklet-size+)))))

(defstruct stack
  (stacklets (error "Foo") :type (array (simple-vector #.+stacklet-size+) 1)
                           :read-only t)
  (top    0 :type (and unsigned-byte fixnum)))

(defun make ()
  (make-stack :stacklets (make-array 16 :fill-pointer 0 :adjustable t)))

(declaim (inline p))
(defun p (x)
  (stack-p x))

(defun %update-stack-top (stack)
  (declare (type stack stack))
  (let ((top (stack-top stack)))
    (when (zerop top)
      (return-from %update-stack-top))
    (multiple-value-bind (major minor) (split-index top)
      (let* ((stacklets (stack-stacklets stack))
             (stacklet  (aref stacklets major))
             (position  (position nil stacklet :from-end t :end minor :test-not #'eql)))
        (cond (position
               (setf (stack-top stack) (+ (* major +stacklet-size+)
                                          position 1)))
              (t
               (setf (stack-top stack) (* major +stacklet-size+))
               (%update-stack-top stack)))))))

(defun %push (stack value)
  (declare (type stack stack) (type (not null) value))
  (%update-stack-top stack)
  (multiple-value-bind (stacklet index)
      (truncate (stack-top stack) +stacklet-size+)
    (let ((stacklets (stack-stacklets stack)))
      (loop while (<= (length stacklets) stacklet)
            do (vector-push-extend (make-array +stacklet-size+ :initial-element nil)
                                   stacklets))
      (let ((stacklet (aref stacklets stacklet)))
        (setf (aref stacklet index) value)
        (incf (stack-top stack))
        value))))

(defun steal (stack)
  (declare (type stack stack))
  (loop repeat (ceiling (stack-top stack) +stacklet-size+)
        for stacklet across (stack-stacklets stack)
        do
           (let ((start 0))
             (loop
              (let* ((position  (position nil stacklet
                                          :start start
                                          :test-not #'eql))
                     (x         (and position
                                     (aref stacklet position))))
                (cond ((null position)
                       (return))
                      ((null x)
                       (setf start (1+ position)))
                      ((consp x)
                       (let ((bulk (cdr x)))
                         (when (and bulk
                                    (plusp (bulk-task-waiting bulk)))
                           (return-from steal bulk))
                         (setf (cdr x) nil))
                       (when (cas (svref stacklet position) x nil)
                         (incf start)))
                      ((eql x (cas (svref stacklet position) x nil))
                       (return-from steal x))))))))

;; bulk tasks are represented, on-stack as conses: the CAR is a hint
;; wrt where to start looking for subtasks, and the CDR is the bulk-task
;; object.  When we're done with the bulk-task, the CDR is NIL.
(declaim (inline bulk-task-hintify))
(defun bulk-task-hintify (x &optional (hint 0))
  (declare (type (real 0 1) hint))
  (etypecase x
    ((or function symbol task) x)
    (bulk-task
     (cons (truncate (* hint (length (bulk-task-subtasks x))))
           x))))

(defun push (stack x &optional (hint 0))
  (%push stack (bulk-task-hintify x hint)))

(defun push-all (stack values &optional (hint 0))
  (map nil (lambda (x)
             (%push stack (bulk-task-hintify x hint)))
       values))

(declaim (inline bulk-find-task))
(defun bulk-find-task (hint-and-bulk)
  (declare (type cons hint-and-bulk))
  (destructuring-bind (hint . bulk) hint-and-bulk
    (declare (type fixnum hint)
             (type (or null bulk-task) bulk))
    (when (null bulk)
      (return-from bulk-find-task))
    (let* ((subtasks (bulk-task-subtasks bulk))
           (begin    hint)
           (end      (length subtasks)))
      (loop
        (when (zerop (bulk-task-waiting bulk))
          (setf (cdr hint-and-bulk) nil)
          (return (values nil nil)))
        (let ((index (position nil subtasks :start begin :end end :test-not #'eql)))
          (cond (index
                 (setf begin (1+ index))
                 (let ((x (aref subtasks index)))
                   (when (and x
                              (eql (cas (svref subtasks index) x nil)
                                   x))
                     (atomic-decf (bulk-task-waiting bulk))
                     (setf (car hint-and-bulk) begin)
                     (return x))))
                ((zerop begin)
                 (setf (cdr hint-and-bulk) nil)
                 (return))
                (t
                 (setf begin 0
                       end   hint))))))))

(defun pop-one-task (stack)
  (declare (type stack stack))
  (loop
    (when (zerop (stack-top stack))
      (return nil))
    (multiple-value-bind (major minor) (split-index (stack-top stack))
      (let* ((stacklets (stack-stacklets stack))
             (stacklet  (aref stacklets major))
             (position  (position nil stacklet :from-end t :end minor :test-not #'eql)))
        (cond (position
               (setf (stack-top stack) (+ (* major +stacklet-size+)
                                          position))
               (let ((x (aref stacklet position)))
                 (etypecase x
                   (null)
                   (cons
                    (let ((bulk-task (cdr x)))
                      (when (and bulk-task
                                 (plusp (bulk-task-waiting bulk-task)))
                        (return x)))
                    (setf (cdr x) nil
                          (svref stacklet position) nil
                          (stack-top stack) (+ (* major +stacklet-size+)
                                               position)))
                   ((or task symbol function)
                    (when (eql (cas (svref stacklet position) x nil) x)
                      (return x))))))
              ((zerop major)
               (setf (stack-top stack) 0)
               (return nil))
              (t
               (setf (stack-top stack) (* major +stacklet-size+))))))))

(defun run-one (stack)
  (let ((task (pop-one-task stack))
        subtask)
    (cond ((not task) nil)
          ((atom task)
           (execute-task task)
           t)
          ((setf subtask (bulk-find-task task))
           (let ((bulk-task (cdr task)))
             (declare (type bulk-task bulk-task))
             (funcall subtask bulk-task)
             (when (= (atomic-decf (bulk-task-remaining bulk-task))
                      1)
               (setf (cdr task) nil)
               (setf (bulk-task-subtasks bulk-task) #())
               (let ((cleanup (bulk-task-cleanup bulk-task)))
                 (etypecase cleanup
                   (null)
                   (cons
                    (dolist (cleanup cleanup)
                      (funcall cleanup bulk-task)))
                   ((or function symbol)
                    (funcall cleanup bulk-task))))
               (setf (bulk-task-cleanup bulk-task) nil)))
           t)
          (t
           (run-one stack)))))
