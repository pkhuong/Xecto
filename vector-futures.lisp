(defpackage "VECTOR-FUTURE"
  (:use "CL" "SB-EXT")
  (:export "VECTOR-FUTURE"
           "RETAIN"
           "RELEASE"
           "MAKE"
           "DATA"))

;;; A vector-future is a parallel future that is also
;;; an on-demand-allocated vector.
;;;
;;; There's also a reference count in prevision of storage reuse
;;; and/or foreign allocation.
;;;
;;; retain/release update the reference count.
;;; data returns the data vector for a vector-future
;;; make creates a new vector future with a refcount of 1.
;;; (make allocation dependencies tasks)
;;;  allocation defines the allocation procedure.
;;;   integer -> make a brand new array
;;;   vector-future: make a copy of the vector
;;; dependencies: list of dependencies (vector-futures)
;;; tasks: vector of functions (subtasks)

(in-package "VECTOR-FUTURE")

(defstruct (vector-future
            (:include parallel-future:future))
  (refcount 0 :type word)
  (size     0 :type (and unsigned-byte fixnum))
  (data   nil :type (or null (simple-array double-float 1))))

(defun data (vector-future)
  (declare (type vector-future vector-future))
  (the (not null)
       (vector-future-data vector-future)))

(defun retain (future)
  (when (zerop (atomic-incf (vector-future-refcount future)))
    (assert (null (vector-future-data future))))
  nil)

(defun release (future)
  (when (= 1 (atomic-decf (vector-future-refcount future)))
    (setf (vector-future-data future) nil))
  nil)

(defun make-allocator (allocation)
  ;; finalize this
  (etypecase allocation
    ((and unsigned-byte fixnum)
     (lambda (data)
       (declare (type vector-future data))
       (retain data) ;; maybe we should just abort here...
       (setf (vector-future-data data)
             (make-array allocation :element-type 'double-float))
       nil))
    (vector-future
     (retain allocation)
     (lambda (data)
       (declare (type vector-future data))
       (retain data)
       (let ((source (vector-future-data allocation)))
         (declare (type (simple-array double-float 1) source))
         (if (= 1 (vector-future-refcount allocation))
             (shiftf (vector-future-data data)
                     (vector-future-data allocation)
                     nil)
             (setf (vector-future-data data)
                   (make-array (length source)
                               :element-type 'double-float
                               :initial-contents source)))
         (release allocation))
       nil))))

(defun make-deallocator (dependencies)
  (map nil #'retain dependencies)
  (lambda (data)
    (declare (type vector-future data))
    (release data)
    (map nil #'release dependencies)))

(defun make (allocation dependencies tasks
             &optional constructor
             &rest     arguments)
  (declare (dynamic-extent arguments))
  (let ((size (if (vector-future-p allocation)
                  (vector-future-size allocation)
                  allocation)))
    (apply 'parallel-future:make
           (coerce (remove-duplicates
                    (if (vector-future-p allocation)
                        (adjoin allocation dependencies)
                        dependencies))
                   'simple-vector) 
           (make-allocator allocation)
           (coerce tasks 'simple-vector)
           (make-deallocator dependencies)
           (or constructor #'make-vector-future)
           :size     size
           :refcount 1
           arguments)))

#||
;; demo

(defun pmap (fun x y)
  (let* ((args (list x y))
         (size (min (vector-future-size x)
                    (vector-future-size y))))
    (make size args
          (loop with step = (max 1 (round size 16))
                for i from 0 below size by step
                collect
                (let ((end (min size (+ i step)))
                      (start i))
                  (lambda (data)
                    (let ((r (vector-future-data data))
                          (x (vector-future-data x))
                          (y (vector-future-data y)))
                      (declare (type (simple-array double-float 1) r x y))
                      (loop for i from start below end
                            do (setf (aref r i)
                                     (funcall fun (aref x i) (aref y i)))))))))))

(defun test (n)
  (let* ((src (make n '() '()))
         (one (pmap (lambda (x y) x y
                      1d0)
                    src src))
         (two (pmap (lambda (x y) x y
                      2d0)
                    src src))
         (three (pmap #'+ one two))
         (four  (pmap #'* two two)))
    (release src)
    (release one)
    (release two)
    (future:wait two :done)
    (format t "rc: ~A ~A~%"
            (vector-future-refcount two)
            (future:status two))
    (sleep 1)
    (future:wait three :done)
    (future:wait four :done)
    (format t "rc: ~A~%" (vector-future-refcount two))
    (values src one two three four)))
||#
