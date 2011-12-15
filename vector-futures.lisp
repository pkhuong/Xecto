(defpackage "VECTOR-FUTURE"
  (:use "CL" "SB-EXT")
  (:export "VECTOR-FUTURE"
           "RETAIN"
           "RELEASE"
           "MAKE"))

(in-package "VECTOR-FUTURE")

(defstruct (vector-future
            (:constructor %make-future-data (&optional (refcount 1)
                                             &aux %parallel-future)))
  (refcount 0 :type word)
  (data   nil :type (or null (simple-array double-float 1)))
  (%parallel-future nil :type parallel-future:future))

(defun retain (future)
  (when (zerop (atomic-incf (vector-future-refcount future)))
    (assert (null (vector-future-data future))))
  nil)

(defun release (future)
  (when (= 1 (atomic-decf (vector-future-refcount future)))
    (setf (vector-future-data future) nil))
  nil)

(defun vector-future-parallel-future (vector-future)
  (vector-future-%parallel-future vector-future))

(defun make-allocator (allocation)
  (etypecase allocation
    ((and unsigned-byte fixnum)
     (lambda (data)
       (declare (type vector-future data))
       (retain data)
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

(defun make (allocation dependencies &rest tasks)
  (declare (dynamic-extent tasks))
  (let ((future (%make-future-data)))
    (setf (vector-future-%parallel-future future)
          (parallel-future:make
           (if (and (vector-future-p allocation)
                    (not (member allocation dependencies)))
               (cons allocation dependencies)
               dependencies)
           (make-allocator allocation)
           tasks
           (make-deallocator dependencies)
           future))
    future))
