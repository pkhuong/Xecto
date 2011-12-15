(defpackage "PARALLEL-FUTURE"
  (:use "CL" "SB-EXT")
  (:export "*CONTEXT*" "WITH-CONTEXT"
           "FUTURE"
           "MAKE"))

(in-package "PARALLEL-FUTURE")

(defvar *context* (wq:make 2))

(defmacro with-context ((count) &body body)
  `(let ((*context* (wq:make ,count)))
     ,@body))

(defstruct (future
            (:constructor
                %make-future (fun dependencies
                              before units after
                              data
                              &aux (left-count (length units))))
            (:include future:future))
  (context *context* :type wq:queue      :read-only t)
  (dependencies  nil :type simple-vector :read-only t)
  (before        nil :type function      :read-only t)
  (units         nil :type simple-vector :read-only t)
  (left-count      0 :type word)
  (after         nil :type function      :read-only t)
  (data          nil :read-only t))

(defun future-fun (future)
  (declare (type future future))
  (funcall (future-before future) (future-data future))
  (if (zerop (length (future-units future)))
      (close-future future)
      (wq:push-self-all (future-context future)
                        (future-units future)))
  nil)

(defun close-future (future)
  (funcall (future-after future) (future-data future))
  (future:mark-done future))

(defstruct (task
            (:constructor %make-task (parent task fun &optional hash))
            (:include wq:task))
  (parent nil :type future   :read-only t)
  (task   nil :type function :read-only t))

(defun task-fun (task)
  (declare (type task task))
  (let ((future (task-parent task)))
    (funcall (task-task task) (future-data future))
    (when (= 1 (atomic-decf (future-left-count future)))
      (close-future future)))
  nil)

(defun wrap-units (future during)
  (map-into (future-units future)
            (lambda (fun)
              (etypecase fun
                (function
                 (%make-task future fun #'task-fun))
                ((cons function (and unsigned-byte fixnum))
                 (%make-task future (car fun) #'task-fun
                             (cdr fun)))))
            during)
  future)

(defun make (dependencies before during after &optional data)
  (let* ((dependencies (make-array (length dependencies)
                                   :initial-contents dependencies))
         (units        (make-array (length during)))
         (future       (%make-future #'future-fun
                                     dependencies
                                     before
                                     units
                                     after
                                     data)))
    (wrap-units future during)
    (future:mark-dependencies future dependencies)
    future))
