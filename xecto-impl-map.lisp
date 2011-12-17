(in-package "XECTO-IMPL")

(defun map-xecto (fun arg &rest args)
  (declare (type xecto arg))
  (let* ((args    (cons arg args))
         (shapes  (mapcar #'xecto-shape args)))
    (multiple-value-bind (r-size r-shape)
        (%canonical-size-and-shape (reduce (lambda (x y)
                                             (if (> (length x) (length y))
                                                 x y))
                                           shapes))
      (map-into shapes (lambda (x)
                         (extend-shape-or-die r-shape x))
                shapes)
      (apply 'execute-map
             fun r-size r-shape
             (apply 'xecto-loop-nest:optimize
                    (cons 0 r-shape)
                    (mapcar (lambda (xecto shape)
                              (cons (xecto-offset xecto) shape))
                            args shapes))
             args))))

(defvar *max-inner-loop-count* (ash 1 16))

(defun compute-map-tasks (function pattern &rest arguments)
  (let ((tasks (make-array 16 :fill-pointer 0 :adjustable t))
        (data  (map 'simple-vector #'xecto-data arguments))
        (max-count *max-inner-loop-count*))
    (destructuring-bind (offsets . loop) pattern
      (declare (type (simple-array index 1) offsets)
               (type simple-vector loop))
      (labels
          ((rec (depth offsets)
             (declare (type (simple-array index 1) offsets))
             (let ((offsets (copy-seq offsets)))
               (destructuring-bind (trip . strides) (aref loop depth)
                 (if (= depth (1- (length loop)))
                     (loop for i below trip by max-count
                           do (let* ((start i)
                                     (count (min max-count
                                                 (- trip start))))
                                (vector-push-extend
                                 (let ((offsets (copy-seq offsets))
                                       (loop    (cons count
                                                      strides)))
                                   (lambda (dst)
                                     (execute-submap dst function
                                                     offsets
                                                     loop
                                                     data)))
                                 tasks)
                                (map-into offsets
                                          (lambda (x inc)
                                            (+ x (* inc max-count)))
                                          offsets strides)))
                     (loop repeat trip do
                       (rec (1+ depth) offsets)
                       (map-into offsets #'+
                                 offsets strides)))))))
        (rec 0 offsets)))
    (nreverse (coerce tasks 'simple-vector))))

(defun execute-submap (destination function offsets loop arguments)
  (declare (type vector-future:vector-future destination)
           (type (simple-array index 1) offsets)
           (type (cons index (simple-array fixnum 1)) loop)
           (type (simple-array vector-future:vector-future 1)
                 arguments))
  (let ((data    (make-array (1+ (length arguments))))
        (offsets (copy-seq offsets)))
    (declare (type (simple-array (simple-array double-float 1) 1) data))
    (setf (aref data 0) (vector-future:data destination))
    (loop for i from 1 below (length data) do
      (setf (aref data i) (vector-future:data (aref arguments (1- i)))))
    (destructuring-bind (repeat . strides) loop
      (if (eql function #'+)
          (loop for i below repeat do
            (setf (aref (aref data 0) (aref offsets 0))
                  (let ((acc 0d0))
                    (declare (optimize speed))
                    (declare (double-float acc))
                    (loop for j from 1 below (length data)
                          do (incf acc (aref (aref data j) (aref offsets j))))
                    acc))
            (map-into offsets #'+ offsets strides))
          (loop for i below repeat do
            (setf (aref (aref data 0) (aref offsets 0))
                  (apply function
                         (loop for j from 1 below (length data)
                               collect (aref (aref data j) (aref offsets j)))))
            (map-into offsets #'+ offsets strides))))))

(defun execute-map (fun r-size r-shape
                    pattern
                    &rest args)
  (let* ((tasks (apply 'compute-map-tasks fun pattern args))
         (data (vector-future:make r-size
                                   (mapcar #'xecto-data args)
                                   tasks)))
    (%make-xecto r-shape data)))
