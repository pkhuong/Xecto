(defpackage "PARALLEL"
  (:use "CL")
  (:export "PROMISE" "PROMISE-VALUE" "PLET"
           "FUTURE" "FUTURE-VALUE" "FUTURE-BIND"
           "PDOTIMES" "PMAP" "PREDUCE"))

(in-package "PARALLEL")

(deftype status ()
  `(member :waiting :done))

(defstruct (promise
            (:constructor make-promise (function))
            (:include work-stack:task))
  %values
  (%status :waiting :type (or status promise-slow-status)))

(status:define-status-type promise-slow-status
    (:fast-type promise
     :status-type status
     :default-status :waiting
     :final-states (:done))
    promise-%status
    promise-status
    %promise-wait
    %promise-upgrade)

(defun promise (thunk &rest args)
  (work-queue:push-self
   (make-promise (lambda (promise)
                   (declare (type promise promise))
                   (setf (promise-%values promise)
                         (multiple-value-list (apply thunk args)))
                   (%promise-upgrade promise :done :waiting)))))

(defun promise-value (promise)
  (declare (type promise promise))
  (when (work-queue:worker-id)
    (work-queue:progress-until
     (lambda ()
       (eql :done (promise-status promise)))))
  (%promise-wait promise :done)
  (values-list (promise-%values promise)))

(defmacro plet ((&rest bindings) &body body)
  (let ((temporaries (loop for (name value) in bindings
                           collect `(,(gensym "PROMISE") (promise (lambda ()
                                                                    ,value))))))
    `(let* (,@temporaries
            ,@(loop for (name) in bindings
                    for (temp) in temporaries
                    collect `(,name (promise-value ,temp))))
       ,@body)))

(defstruct (future
            (:include parallel-future:future))
  %values)

(defun call-with-future-values (function futures)
  (declare (type simple-vector futures))
  (apply function (map 'list (lambda (x)
                               (if (future-p x)
                                   (future-value x)
                                   x))
                       futures)))

(defun future (dependencies callback &key subtasks cleanup)
  (declare (type simple-vector dependencies)
           (type (or null simple-vector) subtasks))
  (let ((future (parallel-future:make
                 (remove-if-not #'future-p dependencies)
                 (lambda (self)
                   (setf (future-%values self)
                         (values-list (call-with-future-values
                                       callback dependencies))))
                 (or subtasks #())
                 (and cleanup
                      (lambda (self)
                        (setf (future-%values self)
                              (values-list (call-with-future-values
                                            cleanup dependencies)))))
                 #'make-future)))
    (work-queue:push-self future)
    future))

(defun future-value (future)
  (declare (type future future))
  (when (work-queue:worker-id)
    (work-queue:progress-until (lambda ()
                                 (eql (future:status future) :done))))
  (future:wait future :done)
  (values-list (future-%values future)))

(defmacro future-bind ((&rest bindings)
                       &body body)
  (let ((wait nil))
    (when (eql :wait (car body))
      (setf wait t)
      (pop body))
    `(,(if wait 'future-value 'identity)
      (future (vector ,@(mapcar #'second bindings))
              (lambda ,@(mapcar #'first bindings)
                ,@body)))))

(defun %call-n-times (count function cleanup)
  (let ((future
          (parallel-future:make
           #()
           nil
           (make-array count :initial-element 0)
           (and cleanup
                (lambda (self)
                  (setf (future-%values self)
                        (values-list (funcall cleanup)))))
           #'make-future
           :%values '(nil)
           :subtask-function (lambda (subtask self index)
                               (declare (ignore subtask self))
                               (funcall function index)))))
    (work-queue:push-self future)
    future))

(defun call-n-times (count function aggregate-function &optional cleanup)
  (let* ((worker-count (or (work-queue:worker-count)
                           (error "No current queue")))
         (max          (expt worker-count 2)))
    (if (<= count max)
        (%call-n-times count function cleanup)
        (let ((step   (truncate count max)))
          (%call-n-times (ceiling count step)
                         (lambda (i)
                           (let* ((begin (* i step))
                                  (end   (min (+ begin step) count)))
                             (funcall aggregate-function begin end)))
                         cleanup)))))

(defmacro pdotimes ((var count &optional result) &body body)
  (let ((begin (gensym "BEGIN"))
        (end   (gensym "END"))
        (i     (gensym "I"))
        (wait  nil)
        (tid   (gensym "TID")))
    (when (eql (car body) :wait)
      (setf wait t)
      (pop body))
    `(,(if wait 'future-value 'identity)
      (call-n-times ,count
                    (lambda (,var)
                      ,@body)
                    (lambda (,begin ,end &aux (,tid (work-queue:worker-id)))
                      (declare (type fixnum ,begin ,end ,tid))
                      (flet ((work-queue:worker-id ()
                               ,tid))
                        (declare (inline work-queue:worker-id)
                                 (ignorable #'work-queue:worker-id))
                        (loop for ,i of-type fixnum from ,begin below ,end
                              do
                                 (let ((,var ,i))
                                   ,@body))))
                    (lambda ()
                      (progn ,result))))))

(defun pmap (type function arg &key (wait t))
  (let* ((arg (coerce arg 'simple-vector))
         (function (if (functionp function)
                       function
                       (fdefinition function)))
         (future (if (eql nil type)
                     (pdotimes (i (length arg))
                       (funcall function (aref arg i)))
                     (let ((destination (make-array (length arg))))
                       (dotimes (i (length arg) (coerce destination type))
                         (setf (aref destination i)
                               (funcall function (aref arg i))))))))
    (if wait
        (future-value future)
        future)))

(defun preduce (function arg seed &key (wait t))
  (let* ((arg (coerce arg 'simple-vector))
         (function (if (functionp function)
                       function
                       (fdefinition function)))
         (accumulators (make-array (work-queue:worker-count)
                                   :initial-element seed))
         (future (dotimes (i (length arg) (reduce function accumulators
                                                  :initial-value seed))
                   (let ((idx (work-queue:worker-id)))
                     (setf (aref accumulators idx)
                           (funcall function
                                    (aref accumulators idx)
                                    (aref arg i)))))))
    (if wait
        (future-value future)
        future)))
