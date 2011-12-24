(defpackage "WORK-UNIT"
  (:use "CL" "SB-EXT")
  (:export "TASK" "TASK-P" "TASK-FUNCTION"
           "BULK-TASK" "BULK-TASK-P" "BULK-TASK-WAITING" "BULK-TASK-REMAINING"
           "BULK-TASK-SUBTASK-FUNCTION" "BULK-TASK-SUBTASKS"
           "BULK-TASK-CLEANUP"
           "TASK-DESIGNATOR"
           "EXECUTE-TASK" "%BULK-FIND-TASK"))

(in-package "WORK-UNIT")

(defstruct (task
            (:constructor nil))
  (function (error "Missing arg") :type (or symbol function)))

(defstruct (bulk-task
            (:constructor nil))
  ;; count waiting to be executed, initially (length subtasks)
  (waiting   (error "Missing arg") :type word)
  ;; count not done yet, initially (length subtasks)
  (remaining (error "Missing arg") :type word)
  (subtask-function nil            :type (or null symbol function))
  (subtasks  (error "Missing arg") :type simple-vector)
  (cleanup   nil                   :type (or list symbol function)))

(deftype task-designator ()
  `(or (and symbol (not null)) function task bulk-task))

(defun execute-task (task)
  (etypecase task
    ((or symbol function)
     (funcall task))
    (task
     (let ((function (task-function task)))
       (when (and function
                  (eql (cas (task-function task) function nil)
                       function))
         (funcall function task))))))

(declaim (inline random-bit))
(defun random-bit (state max)
  (return-from random-bit 0)
  (let ((random (logand (1- (ash 1 (integer-length max)))
                        (random (1+ most-positive-fixnum)
                                state))))
    (if (zerop random)
        0
        (logxor random (1- random)))))

(declaim (inline %bulk-find-task))
(defun %bulk-find-task (bulk hint random)
  (declare (type fixnum hint)
           (type (or null bulk-task) bulk)
           (type random-state random))
  (when (null bulk)
    (return-from %bulk-find-task (values nil nil)))
  (let* ((subtasks (bulk-task-subtasks bulk))
         (begin    hint)
         (end      (length subtasks)))
    (flet ((acquire (index)
             (let ((x (aref subtasks index)))
               (when (and x
                          (eql (cas (svref subtasks index) x nil)
                               x))
                 (atomic-decf (bulk-task-waiting bulk))
                 (return-from %bulk-find-task
                   (values x index)))))
           (quick-check ()
             (when (zerop (bulk-task-waiting bulk))
               (return-from %bulk-find-task
                 (values nil nil)))))
      (declare (inline acquire quick-check))
      (when (>= begin end)
        (setf begin 0))
      (quick-check)
      (acquire begin)
      (setf begin (mod (logxor begin (random-bit random end))
                       end))
      (loop
        (quick-check)
        (let ((index (position nil subtasks :start begin :end end :test-not #'eql)))
          (cond (index
                 (setf begin (1+ index))
                 (acquire index))
                ((zerop begin)
                 (return (values nil nil)))
                (t
                 (setf begin 0
                       end   hint))))))))

