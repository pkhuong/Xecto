(in-package "XECTO-IMPL")

(defun reduce-xecto (fun arg)
  (declare (type xecto arg))
  (let* ((shape (xecto-shape arg))
         (spine (aref shape 0))
         (bulk  (remove-index shape 0)))
    (multiple-value-bind (r-size r-shape)
        (%canonical-size-and-shape bulk)
      (execute-reduce fun r-size r-shape
                      spine
                      (xecto-loop-nest:optimize (cons 0 r-shape)
                                                (cons (xecto-offset arg) bulk))
                      arg))))

(defun compute-reduce-tasks (function spine pattern arg)
  (let ((tasks '())
        (data  (xecto-data arg)))
    (destructuring-bind (offsets . loop) pattern
      (declare (type (simple-array index (2)) offsets)
               (type simple-vector loop))
      (labels
          ((rec (depth offsets)
             (declare (type (simple-array index (2)) offsets))
             (let ((offsets (copy-seq offsets)))
               (if (= depth (length loop))
                   (push (let ((offsets (copy-seq offsets)))
                           (lambda (dst)
                             (execute-subreduce dst function spine
                                                offsets data)))
                         tasks)
                   (destructuring-bind (trip . strides) (aref loop depth)
                     (declare (type (simple-array fixnum (2)) strides))
                     (loop repeat trip do
                       (rec (1+ depth) offsets)
                       (map-into offsets #'+
                                 offsets strides)))))))
        (rec 0 offsets)))
    (nreverse tasks)))

(defun execute-subreduce (destination function spine offsets arg)
  (declare (type vector-future:vector-future destination arg)
           (type (simple-array index (2)) offsets))
  (destructuring-bind (repeat . stride) spine
    (declare (type index repeat stride))
    (let* ((dst-vec (vector-future:data destination))
           (dst-off (aref offsets 0))
           (src-vec (vector-future:data arg))
           (src-off (aref offsets 1))
           (acc     (aref src-vec src-off)))
      (declare (type double-float acc)
               (type index dst-off src-off))
      (if (eql function #'+)
          (loop repeat (1- repeat) do
            (incf src-off stride)
            (incf acc (aref src-vec src-off)))
          (loop repeat (1- repeat) do
            (incf src-off stride)
            (setf acc (funcall function acc (aref src-vec src-off)))))
      (setf (aref dst-vec dst-off) acc))))

(defun execute-reduce (fun r-size r-shape spine pattern arg)
  (let* ((tasks (compute-reduce-tasks fun spine pattern arg))
         (data  (apply 'vector-future:make
                       r-size
                       (list (xecto-data arg))
                       tasks)))
    (%make-xecto r-shape data)))
