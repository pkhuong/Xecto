(defpackage "WORK-STACK"
  (:use "CL" "SB-EXT" "SB-THREAD" "WORK-UNIT")
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
  (top    0 :type (and unsigned-byte fixnum))
  (bottom 0 :type (and unsigned-byte fixnum)))

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
  (labels ((update-bottom (i)
             (when (/= i (stack-bottom stack))
               (setf (stack-bottom stack) i)))
           (sub-steal (begin end)
             (declare (type (and fixnum unsigned-byte) begin end))
             (loop with stacklets = (stack-stacklets stack)
                   for i from begin below (max end (length stacklets))
                   for stacklet = (aref stacklets i)
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
                                       (update-bottom i)
                                       (return-from steal bulk)))
                                   (setf (cdr x) nil)
                                   (setf start position)
                                   (when (eql x (cas (svref stacklet position) x nil))
                                     (incf start)))
                                  ((eql x (cas (svref stacklet position) x nil))
                                   (update-bottom i)
                                   (return-from steal x)))))))))
    (declare (inline update-bottom))
    (let ((bottom (stack-bottom stack))
          (top    (ceiling (stack-top stack) +stacklet-size+)))
      (cond ((>= bottom top)
             (update-bottom 0)
             (sub-steal 0 top))
            (t
             (sub-steal bottom top)
             (sub-steal 0 bottom)
             (update-bottom 0)
             nil)))))

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
  (when x
    (%push stack (bulk-task-hintify x hint))))

(defun push-all (stack values &optional (hint 0))
  (map nil (lambda (x)
             (when x
               (%push stack (bulk-task-hintify x hint))))
       values))

(defun pop-one-task (stack)
  (declare (type stack stack))
  (loop
    (when (zerop (stack-top stack))
      (return nil))
    (multiple-value-bind (major minor) (split-index (stack-top stack))
      (let* ((stacklets (stack-stacklets stack))
             (stacklet  (aref stacklets major))
             (position  (position nil stacklet :from-end t
                                               :end minor
                                               :test-not #'eql)))
        (cond (position
               (let ((x (aref stacklet position)))
                 (etypecase x
                   (null)
                   (cons
                    (let ((bulk-task (cdr x)))
                      (when (and bulk-task
                                 (plusp (bulk-task-waiting bulk-task)))
                        (setf (stack-top stack) (+ 1 (* major +stacklet-size+)
                                                   position))
                        (return x)))
                    (setf (cdr x) nil
                          (svref stacklet position) nil
                          (stack-top stack) (+ (* major +stacklet-size+)
                                               position))
                    (barrier (:memory)))
                   ((or task symbol function)
                    (setf (stack-top stack) (+ (* major +stacklet-size+)
                                               position))
                    (when (eql (cas (svref stacklet position) x nil) x)
                      (return x))))))
              ((zerop major)
               (setf (stack-top stack) 0)
               (return nil))
              (t
               (setf (stack-top stack) (* major +stacklet-size+))))))))

(declaim (inline bulk-find-task))
(defun bulk-find-task (hint-and-bulk random-state)
  (declare (type cons hint-and-bulk))
  (destructuring-bind (hint . bulk) hint-and-bulk
    (declare (type fixnum hint)
             (type (or null bulk-task) bulk))
    (when (null bulk)
      (return-from bulk-find-task (values nil nil nil)))
    (multiple-value-bind (task index)
        (%bulk-find-task bulk hint random-state)
      (cond (task
             (setf (car hint-and-bulk) index)
             (values task index bulk))
            (t
             (setf (cdr hint-and-bulk) nil)
             (values nil nil nil))))))

(defun run-one (stack random-state)
  (let ((task (pop-one-task stack))
        subtask subtask-index bulk-task)
    (cond ((not task) nil)
          ((atom task)
           (execute-task task)
           t)
          ((setf (values subtask subtask-index bulk-task)
                 (bulk-find-task task random-state))
           (let* ((bulk-task bulk-task)
                  (function (bulk-task-subtask-function bulk-task)))
             (declare (type bulk-task bulk-task))
             (if function
                 (funcall function subtask bulk-task subtask-index)
                 (funcall subtask bulk-task subtask-index))
             (when (= (atomic-decf (bulk-task-remaining bulk-task))
                      1)
               (setf (cdr task) nil)
               (setf (bulk-task-subtasks bulk-task) #()
                     (bulk-task-subtask-function bulk-task) nil)
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
           (run-one stack random-state)))))
