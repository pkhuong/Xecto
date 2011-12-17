(defpackage "PARALLEL-FUTURE"
  (:use "CL" "SB-EXT")
  (:export "*CONTEXT*" "WITH-CONTEXT"
           "FUTURE" "P"
           "MAKE"))

;;; Parallel futures: hooking up futures with the work queue
;;;
;;; A parallel is a future with an execution in three periods:
;;;  - a list designator of setup functions
;;;  - a vector of subtasks to execute in parallel
;;;  - a list designator of cleanup functions
;;;
;;; When a parallel future is ready for execution, a task that
;;; executes the setup functions and pushes the subtasks to
;;; the local stack is created.  That task is enqueued or pushed
;;; to the local stack.
;;; Once the setup functions have been executed, the subtasks are
;;; pushed as a bulk-task.
;;; Once the bulk-task is completed, the cleanup functions are executed,
;;; and the future is marked as done.

(in-package "PARALLEL-FUTURE")

(defvar *context* (work-queue:make 2 nil :bindings `((*context* . ,#'work-queue:current-queue))))

(defmacro with-context ((count) &body body)
  (let ((context (gensym "CONTEXT")))
    `(let* ((,context  (work-queue:make ,count nil :bindings `((*context* . ,#'work-queue:current-queue))))
            (*context* ,context))
       (unwind-protect (progn
                         ,@body)
         (work-queue:stop ,context)))))

(defstruct (future
            (:include future:future))
  (setup nil :type (or list symbol function)))

(declaim (inline p))
(defun p (x)
  (future-p x))

(defun map-list-designator (functions argument)
  (etypecase functions
    (null)
    (list
     (dolist (function functions)
       (funcall function argument)))
    ((or symbol function)
     (funcall functions argument)))
  nil)

(defun future-push-self (future)
  (declare (type future future))
  (let ((setup (future-setup future)))
    (setf (future-setup future) nil)
    (work-queue:push-self
     (lambda ()
       (map-list-designator setup future)
       (cond ((plusp (future-waiting future))
              (work-queue:push-self future))
             ((zerop (future-remaining future))
              (map-list-designator (future-cleanup future) future)
              (setf (future-cleanup future) nil))
             (t (error "Mu?"))))
     *context*)))

(defun make (dependencies setup subtasks cleanup &optional constructor &rest arguments)
  (declare (type simple-vector dependencies subtasks))
  (let* ((count        (length subtasks))
         (future       (apply (or constructor #'make-future)
                              :function #'future-push-self
                              :dependencies dependencies
                              :setup     setup
                              :subtasks  subtasks
                              :waiting   count
                              :remaining count
                              :cleanup   (if (listp cleanup)
                                             (append cleanup (list #'future:mark-done))
                                             (list cleanup #'future:mark-done))
                              arguments)))
    (future:mark-dependencies future)
    (future:thaw future)))
