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
;;; Normal task-stealing stack, with special support for tasks composed of subtasks.
;;;
;;; A task designator is either a function designator, a task, or a bulk-task.
;;;
;;; A function designator is called, and a task's fun is called with the task as its
;;; only argument.
;;;
;;; When only those are used, the work stack is a normal stack of task units, with
;;; PUSH to insert a new task (PUSH-ALL to insert a sequence of tasks), STEAL to get
;;; one task from the bottom of the stack, and RUN-ONE to execute and pop the topmost
;;; task.
;;;
;;; Bulk-task objects represent a set of subtasks to be executed, and a sequence of
;;; operations to perform once all the subtasks have been completed.
;;;
;;; Task stealing of bulk tasks is special: bulk tasks have multiple owners, so bulk
;;; tasks aren't stolen as much as forcibly shared.  All the workers that share a bulk
;;; task cooperate to complete the subtasks.  The last worker to finish executing a
;;; subtask then executes the cleanups.
;;;
;;; Subtasks and cleanups are functions that are called with the subtask as their one
;;; argument.

(in-package "WORK-STACK")

(defstruct (task
            (:constructor nil))
  (fun (error "Missing arg") :type (or symbol function)
                             :read-only t))

(defstruct (bulk-task
            (:constructor nil))
  ;; count waiting to be executed, initially (length subtasks)
  (waiting   (error "Missing arg") :type word)
  ;; count not done yet, initially (length subtasks)
  (remaining (error "Missing arg") :type word)
  (subtasks  (error "Missing arg") :type (simple-array (or symbol function) 1)
                                   :read-only t)
  (cleanup   nil                   :type (or list function)
                                   :read-only t))

(deftype task-designator ()
  `(or symbol function task bulk-task))

(defun execute-task (task)
  (etypecase task
    ((or symbol function)
     (funcall task))
    (task
     (funcall (task-fun task) task))))

(defstruct stack
  (lock   (make-mutex) :type mutex
                       :read-only t)
  (data   (make-array 32 :fill-pointer 0 :adjustable t)
                       :type (and (not simple-vector)
                                  vector)
                       :read-only t))

(defun make ()
  (make-stack))

(declaim (inline p))
(defun p (x)
  (stack-p x))

(declaim (inline bulk-task-hintify))
(defun bulk-task-hintify (x &optional (hint 0))
  (etypecase x
    ((or function symbol task) x)
    (bulk-task
     (cons (truncate (* hint (length (bulk-task-subtasks x))))
           x))))

(defun push (stack x &optional (hint 0))
  (with-mutex ((stack-lock stack))
    (vector-push-extend (bulk-task-hintify x hint)
                        (stack-data stack))))

(defun push-all (stack values &optional (hint 0))
  (with-mutex ((stack-lock stack))
    (let ((data (stack-data stack)))
      (map nil (lambda (x)
                 (vector-push-extend (bulk-task-hintify x hint)
                                     data))
           values))))

(defun steal (stack)
  (with-mutex ((stack-lock stack))
    (let ((data (stack-data stack)))
      (loop for i below (length data)
            for x = (aref data i)
            do (when (consp x)
                 (setf x (cdr x)))
               (etypecase x
                 (null)
                 ((or symbol function task)
                  (shiftf (aref data i) nil)
                  (return x))
                 (bulk-task
                  (if (plusp (bulk-task-waiting x))
                      (return x)
                      (setf (aref data i) nil))))))))

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
                   (when (eql (cas (svref subtasks index) x nil)
                              x)
                     (atomic-decf (bulk-task-waiting bulk))
                     (setf (car hint-and-bulk) begin)
                     (return x))))
                ((zerop begin)
                 (setf (cdr hint-and-bulk) nil)
                 (return))
                (t
                 (setf begin 0
                       end   hint))))))))

(defun get-one-task (stack)
  (with-mutex ((stack-lock stack))
    (let ((data (stack-data stack)))
      (loop
        (let* ((index (position nil data :from-end t
                                         :test-not #'eql))
               (task  (and index (aref data index))))
          (flet ((clear ()
                   (setf (aref data index)   nil
                         (fill-pointer data) index)))
            (declare (inline clear))
            (etypecase task
              (null
               (setf (fill-pointer data) 0)
               (return nil))
              (cons
               (let ((bulk-task (cdr task)))
                 (when (and bulk-task
                            (plusp (bulk-task-waiting bulk-task)))
                   (return task))
                 (clear)))
              ((or task symbol function)
               (clear)
               (return task)))))))))

(defun run-one (stack)
  (let ((task (get-one-task stack))
        subtask)
    (cond ((not task) nil)
          ((atom task)
           (execute-task task)
           t)
          ((setf subtask (bulk-find-task task))
           (let ((bulk-task (cdr task)))
             (funcall subtask bulk-task)
             (when (= (atomic-decf (bulk-task-remaining bulk-task))
                      1)
               (let ((cleanup (bulk-task-cleanup bulk-task)))
                 (etypecase cleanup
                   (null)
                   (cons
                    (dolist (cleanup cleanup)
                      (funcall cleanup bulk-task)))
                   ((or function symbol)
                    (funcall cleanup bulk-task))))))
           t)
          (t
           (run-one stack)))))
