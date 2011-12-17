(defpackage "FUTURE"
  (:use "CL" "SB-EXT" "SB-THREAD")
  (:export "FUTURE" "TASK-FUTURE" "BULK-FUTURE"
           "DEPENDENTS"
           "STATUS" "WAIT" "CANCEL"
           "MARK-DEPENDENCIES" "MARK-DONE"))

(in-package "FUTURE")
(deftype status ()
  '(member :frozen :waiting :running :done :cancelled))

(defstruct (slow-status
            (:constructor
                make-slow-status (&optional (status :frozen))))
  (status :waiting         :type status)
  (lock   (make-mutex)     :type mutex
                           :read-only t)
  (cvar   (make-waitqueue) :type waitqueue
                           :read-only t))

(defstruct (future
            (:include work-stack:bulk-task)
            (:constructor nil))
  (antideps     nil :type (or list (member :done :cancelled)))
  (waitcount      0 :type word)
  (%status  :frozen :type (or status slow-status)))

(defun dependents (future)
  (let ((antideps (future-antideps future)))
    (and (listp antideps)
         antideps)))

(defun status (future)
  (declare (type future future))
  (let ((%status (future-%status future)))
    (if (slow-status-p %status)
        (slow-status-status %status)
        %status)))

(defun wait (future &rest stopping-conditions)
  (declare (dynamic-extent stopping-conditions))
  (loop
    (let ((%status (future-%status future)))
      (when (slow-status-p %status)
        (return))
      (when (member %status stopping-conditions)
        (return-from wait %status))
      (let ((new-status (make-slow-status %status)))
        (when (eql (compare-and-swap (future-%status future)
                                     %status new-status)
                   %status)
          (return)))))
  (let* ((slow-status (future-%status future))
         (lock        (slow-status-lock slow-status))
         (cvar        (slow-status-cvar slow-status)))
    (declare (type slow-status slow-status))
    (with-mutex (lock)
      (loop
        (let ((status (slow-status-status slow-status)))
          (when (member status stopping-conditions)
            (return status)))
        (condition-wait cvar lock)))))

(defun status-upgrade (future to &rest from)
  (declare (dynamic-extent from))
  (loop
    (let ((%status (future-%status future)))
      (when (slow-status-p %status)
        (return))
      (when (or (not (member %status from))
                (eql (compare-and-swap (future-%status future)
                                       %status to)
                     %status))
        (return-from status-upgrade %status))))
  (let ((slow-status (future-%status future)))
    (with-mutex ((slow-status-lock slow-status))
      (let ((status (slow-status-status slow-status)))
        (when (member status from)
          (setf (slow-status-status slow-status) to)
          (when (or (eql to :done) (eql to :cancelled))
            (setf (future-%status future) to))
          (condition-broadcast (slow-status-cvar slow-status)))
        status))))

(defun execute (future)
  (unless (eql (status-upgrade future :running :waiting)
               :waiting)
    (return-from execute))
  (funcall (future-fun future) future))

(defun cancel (future)
  (declare (type future future))
  (let ((status (status-upgrade future :cancelled :waiting)))
    (when (eql status :waiting)
      ;; recursively mark as cancelled?
      (setf (future-antideps future) :cancelled))
    status))

(defun mark-dependencies (future dependencies)
  (declare (type future future))
  (let ((wp          (make-weak-pointer future))
        (any-waiting nil))
    (flet ((mark-dep (dep)
             (declare (type future dep))
             (ecase (status dep)
               ((:waiting :running))
               (:done
                (return-from mark-dep))
               (:cancelled
                (error "Dependency cancelled")))
             (let ((cons (list wp)))
               (atomic-incf (future-waitcount future))
               (loop
                 (let ((antideps (future-antideps dep)))
                   (setf (cdr cons) antideps)
                   (cond ((eql antideps :done)
                          (atomic-decf (future-waitcount future))
                          (return-from mark-dep))
                         ((eql antideps :cancelled)
                          ;; cancel self?
                          (atomic-decf (future-waitcount future))
                          (error "Dependency cancelled"))
                         ((eql (compare-and-swap (future-antideps dep)
                                                 antideps
                                                 cons)
                               antideps)
                          (setf any-waiting nil)
                          (return-from mark-dep))))))))
      (declare (dynamic-extent #'mark-dep))
      (map nil #'mark-dep dependencies)
      (unless any-waiting
        (execute future)))))

(defun mark-done (future)
  (declare (type future future))
  (unless (eql :running (status-upgrade future :done :running))
    (return-from mark-done))
  (let ((antideps
          (loop
            (let ((antideps (future-antideps future)))
              (when (or (eql antideps :done)
                         (eql antideps :cancelled))
                (return-from mark-done))
              (when (eql (compare-and-swap (future-antideps future)
                                           antideps :done)
                         antideps)
                (return antideps))))))
    (map-into antideps
              (lambda (wp)
                (let ((value (weak-pointer-value wp)))
                  (when (and value
                             (eql :waiting (status value))
                             (= 1 (atomic-decf (future-waitcount value))))
                    (execute value)))
                nil)
              antideps)
    nil))
