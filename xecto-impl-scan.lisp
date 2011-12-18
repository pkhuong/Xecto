(in-package "XECTO-IMPL")

(defun scan-xecto (fun arg)
  (declare (type xecto arg))
  (let ((shape (xecto-shape arg)))
    (multiple-value-bind (r-size r-shape)
        (%canonical-size-and-shape shape)
      (let ((src-spine (aref shape   0))
            (dst-spine (aref r-shape 0))
            (src-bulk  (remove-index shape 0))
            (dst-bulk  (remove-index r-shape 0)))
        (assert (eql (car src-spine) (car dst-spine)))
        (execute-scan fun r-size r-shape
                      (list (car dst-spine) (cdr dst-spine) (cdr src-spine))
                      (xecto-loop-nest:optimize (cons 0 dst-bulk)
                                                (cons (xecto-offset arg) src-bulk))
                      arg)))))

(defun compute-scan-tasks (function spine pattern arg)
  (let ((tasks (make-array 16 :fill-pointer 0 :adjustable t))
        (data  (xecto-data arg)))
    (destructuring-bind (offsets . loop) pattern
      (declare (type (simple-array index (2)) offsets)
               (type simple-vector loop))
      (labels
          ((rec (depth offsets)
             (declare (type (simple-array index (2)) offsets))
             (let ((offsets (copy-seq offsets)))
               (if (= depth (length loop))
                   (vector-push-extend
                    (let ((offsets (copy-seq offsets)))
                      (lambda (dst index) index
                        (execute-subscan dst function spine
                                         offsets data)))
                    tasks)
                   (destructuring-bind (trip . strides) (aref loop depth)
                     (declare (type (simple-array fixnum (2)) strides))
                     (loop repeat trip do
                       (rec (1+ depth) offsets)
                       (map-into offsets #'+
                                 offsets strides)))))))
        (rec 0 offsets)))
    (coerce tasks 'simple-vector)))

(defun execute-subscan (destination function spine offsets arg)
  (declare (type vector-future:vector-future destination arg)
           (type (simple-array index (2)) offsets))
  (destructuring-bind (repeat dst-stride src-stride) spine
    (declare (type index repeat)
             (type fixnum dst-stride src-stride))
    (let* ((dst-vec (vector-future:data destination))
           (dst-off (aref offsets 0))
           (src-vec (vector-future:data arg))
           (src-off (aref offsets 1))
           (acc     (aref src-vec src-off)))
      (declare (type double-float acc)
               (type index dst-off src-off))
      (setf (aref dst-vec dst-off) acc)
      (if (eql function #'+)
          (loop repeat (1- repeat) do
            (incf src-off src-stride)
            (incf dst-off dst-stride)
            (setf acc (setf (aref dst-vec dst-off)
                            (+ acc (aref src-vec src-off)))))
          (loop repeat (1- repeat) do
            (incf src-off src-stride)
            (incf dst-off dst-stride)
            (setf (aref dst-vec dst-off) acc)
            (setf acc (funcall function acc (aref src-vec src-off))))))))

(defun execute-scan (fun r-size r-shape spine pattern arg)
  (let* ((tasks (compute-scan-tasks fun spine pattern arg))
         (data  (vector-future:make r-size
                                    (list (xecto-data arg))
                                    tasks)))
    (%make-xecto r-shape data)))
