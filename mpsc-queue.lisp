(defpackage "MPSC-QUEUE"
  (:use "CL" "SB-EXT")
  (:shadow cl:get)
  (:export "QUEUE" "P" "MAKE" "PUT" "GET" "P"))

(in-package "MPSC-QUEUE")

(defstruct (queue
            (:constructor %make-queue (head)))
  (head nil :type list)
  (tail nil :type list))

(declaim (inline p))
(defun p (x)
  (queue-p x))

(defun slow-get (queue)
  (declare (type queue queue))
  (let ((head (queue-head queue)))
    (when head (return-from slow-get head)))
  (let ((tail (loop ; stupid. It's just an xchg
                (let ((tail (queue-tail queue)))
                  (when (eql (cas (queue-tail queue) tail nil)
                             tail)
                    (return tail))))))
    (setf (queue-head queue) (reverse tail))))

(declaim (inline get put))
(defun get (queue &optional default)
  (declare (type queue queue))
  (let ((head (queue-head queue)))
    (cond ((or head
               (setf head (slow-get queue)))
           (destructuring-bind (value . next) head
             (setf (queue-head queue) next
                   (car head)         nil
                   (cdr head)         nil)
             (values value t)))
          (t
           (values default nil)))))

(defun put (queue value)
  (declare (type queue queue))
  (let ((cons (list value)))
    (loop
      (let ((tail (queue-tail queue)))
        (setf (cdr cons) tail)
        (when (eql tail (cas (queue-tail queue) tail cons))
          (return value))))))
(declaim (notinline get put))

(defun make (&optional initial-contents constructor &rest args)
  (let ((contents (coerce initial-contents 'list)))
    (if constructor
        (apply constructor :head contents :tail nil args)
        (%make-queue initial-contents))))
