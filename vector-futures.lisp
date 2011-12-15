(defpackage "VECTOR-FUTURE"
  (:use "CL" "SB-EXT")
  (:export "VECTOR-FUTURE"
           "RETAIN"
           "RELEASE"
           "MAKE"
           "WAIT"
           "DATA"
           "STATUS"))

(in-package "VECTOR-FUTURE")

(defstruct (vector-future
            (:constructor %make-future-data (size
                                             &optional (refcount 1)
                                             &aux %parallel-future)))
  (refcount 0 :type word)
  (size     0 :type (and unsigned-byte fixnum))
  (data   nil :type (or null (simple-array double-float 1)))
  (%parallel-future nil :type parallel-future:future))

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

(defun vector-future-parallel-future (vector-future)
  (vector-future-%parallel-future vector-future))

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

(defun make (allocation dependencies &rest tasks)
  (declare (dynamic-extent tasks))
  (let* ((size   (if (vector-future-p allocation)
                     (vector-future-size allocation)
                     allocation))
         (future (%make-future-data size)))
    (setf (vector-future-%parallel-future future)
          (parallel-future:make
           (mapcar #'vector-future-parallel-future
                   (if (vector-future-p allocation)
                       (adjoin allocation dependencies)
                       dependencies))
           (make-allocator allocation)
           tasks
           (make-deallocator dependencies)
           future))
    future))

(defun wait (future &rest status)
  (declare (dynamic-extent status))
  (apply 'future:wait
         (vector-future-parallel-future future)
         status))

(defun status (future)
  (future:status (vector-future-parallel-future future)))

#||
;; demo

(defun pmap (fun x y)
  (let* ((args (list x y))
         (size (min (vector-future-size x)
                    (vector-future-size y))))
    (apply 'make size args
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
  (let* ((src (make n '()))
         (one (pmap (lambda (x y) x y
                      1d0)
                    src src))
         (two (pmap (lambda (x y)
                      2d0)
                    src src))
         (three (pmap #'+ one two))
         (four  (pmap #'* two two)))
    (release src)
    (release one)
    (release two)
    (wait two :done)
    (format t "rc: ~A ~A~%"
            (vector-future-refcount two)
            (status two))
    (sleep 1)
    (wait three :done)
    (wait four :done)
    (format t "rc: ~A~%" (vector-future-refcount two))
    (values src one two three four)))
||#
