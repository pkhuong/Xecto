(defpackage "STATUS"
  (:use "CL" "SB-EXT" "SB-THREAD")
  (:export "SLOW-STATUS" "DEFINE-STATUS-TYPE"))

(in-package "STATUS")

(defstruct (slow-status
            (:constructor nil))
  (status nil              :type t)
  (lock   (make-mutex)     :type mutex
                           :read-only t)
  (cvar   (make-waitqueue) :type waitqueue
                           :read-only t))

(defmacro define-status-type (type-name
                              (&key (fast-type t)
                                    (status-type t)
                                    (default-status '(error "Status missing"))
                                    (constructor
                                     (intern
                                      (format nil "MAKE-~A"
                                              (symbol-name type-name))))
                                    (final-states '()))
                              fast-accessor
                              status-function
                              wait-function
                              upgrade-function)
  `(progn
     (defstruct (,type-name
                 (:constructor ,constructor)
                 (:include slow-status
                  (status ,default-status :type ,status-type))))

     (defun ,status-function (value)
       (declare (type ,fast-type value))
       (let ((%status (,fast-accessor value)))
         (if (typep %status ',type-name)
             (slow-status-status %status)
             %status)))

     (defun ,wait-function (value &rest stopping-conditions)
       (declare (type ,fast-type value))
       (declare (dynamic-extent stopping-conditions))
       (let (slow)
         (loop
           (let ((%status (,fast-accessor value)))
             (when (typep %status ',type-name)
               (return))
             (when (member %status stopping-conditions)
               (return-from ,wait-function %status))
             (if slow
                 (setf (slow-status-status slow) %status)
                 (setf slow (,constructor :status %status)))
             (when (eql (cas (,fast-accessor value) %status slow)
                        %status)
               (return)))))
       (let* ((slow-status (,fast-accessor value))
              (lock        (slow-status-lock slow-status))
              (cvar        (slow-status-cvar slow-status)))
         (declare (type slow-status slow-status))
         (with-mutex (lock)
           (loop
             (let ((status (slow-status-status slow-status)))
               (when (member status stopping-conditions)
                 (return status)))
             (condition-wait cvar lock)))))

     (defun ,upgrade-function (value to &rest from)
       (declare (type ,fast-type value))
       (declare (dynamic-extent from))
       (loop
         (let ((%status (,fast-accessor value)))
           (when (typep %status ',type-name)
             (return))
           (when (or (not (member %status from))
                     (eql (compare-and-swap (,fast-accessor value)
                                            %status to)
                          %status))
             (return-from ,upgrade-function %status))))
       (let ((slow-status (,fast-accessor value)))
         (declare (type slow-status slow-status))
         (with-mutex ((slow-status-lock slow-status))
           (let ((status (slow-status-status slow-status)))
             (when (member status from)
               (setf (slow-status-status slow-status) to)
               (when (or ,@(mapcar (lambda (x)
                                     `(eql to ',x))
                                   final-states))
                 (setf (,fast-accessor value) to))
               (condition-broadcast (slow-status-cvar slow-status)))
             status))))))
