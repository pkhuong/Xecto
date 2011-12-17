(defpackage "WORK-STACK"
  (:use "CL" "SB-EXT" "SB-THREAD")
  (:shadow cl:push)
  (:export "TASK" "TASK-P" "BULK-TASK" "BULK-TASK-P"
           "TASK-DESIGNATOR"
           "EXECUTE-TASK"
           "STACK" "MAKE" "P"
           "PUSH" "PUSH-ALL" "STEAL" "RUN-ONE"))

(in-package "WORK-STACK")

(defstruct (task
            (:constructor nil))
  (fun (error "Missing arg") :type (or symbol function)
                             :read-only t))

(defstruct (bulk-task
            (:include task)
            (:constructor nil))
  ;; count waiting to be executed
  (waiting   (error "Missing arg") :type word)
  ;; count not done yet
  (remaining (error "Missing arg") :type word)
  (subtasks  (error "Missing arg") :type (simple-array (or symbol function) 1)
                                   :read-only t))

(deftype task-designator ()
  `(or symbol function task bulk-task))

(defun execute-task (task)
  (etypecase task
    ((or symbol function)
     (funcall task))
    (bulk-task
     (error "~S of ~S not supported" 'execute-task 'bulk-task))
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

(defun bulk-task-hintify (x &optional (hint 0))
  (if (bulk-task-p x)
      (cons (truncate (* hint (length (bulk-task-subtasks x))))
            x)
      x))

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
               (cond ((null x))
                     ((not (bulk-task-p x))
                      (shiftf (aref data i) nil)
                      (return x))
                     ((plusp (bulk-task-waiting x))
                      (return x))
                     (t
                      (setf (aref data i) nil)))))))

(defun bulk-find-task (hint-and-bulk)
  (declare (type cons hint-and-bulk))
  (destructuring-bind (hint . bulk) hint-and-bulk
    (declare (type fixnum hint)
             (type bulk-task bulk))
    (let ((subtasks (bulk-task-subtasks bulk)))
      (loop
        (when (zerop (bulk-task-waiting bulk))
          (return (values nil nil)))
        (let ((index (position nil subtasks :start hint :test-not #'eql)))
          (cond (index
                 (let ((x (aref subtasks index)))
                   (when (eql (cas (svref subtasks index) x nil)
                              x)
                     (atomic-decf (bulk-task-waiting bulk))
                     (setf (car hint-and-bulk) index)
                     (return (values x index)))
                   (setf hint index)))
                ((zerop hint)
                 (setf (car hint-and-bulk) 0)
                 (return (values nil nil)))
                (t
                 (setf hint 0))))))))

(defun run-one (stack)
  (with-mutex ((stack-lock stack))
    (let ((data (stack-data stack)))
      (loop
       (let* ((index (position nil data :from-end t
                                        :test-not #'eql))
              (task  (and index (aref data index))))
         (etypecase task
           (null
            (setf (fill-pointer data) 0)
            (return nil))
           (cons
            (multiple-value-bind (subtask index)
                (bulk-find-task task)
              (if subtask
                  (let ((bulk-task (cdr task)))
                    (funcall subtask bulk-task)
                    (when (= (atomic-decf (bulk-task-remaining bulk-task))
                             1)
                      (funcall (bulk-task-fun bulk-task) bulk-task))
                    (return t))
                  (setf (aref data index)   nil
                        (fill-pointer data) index))))
           ((or task symbol function)
            (setf (aref data index) nil
                  (fill-pointer data) index)
            (if (task-p task)
                (funcall (task-fun task) task)
                (funcall task))
            (return t))))))))
