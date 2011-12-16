(in-package "XECTO-IMPL")

(defun transpose (xecto i j)
  (declare (type xecto xecto)
           (type index i j))
  (when (= i j)
    (return-from transpose xecto))
  (when (> i j)
    (rotatef i j))
  (let ((shape (copy-seq (xecto-shape xecto))))
    (assert (< j (length shape)))
    (let ((last (aref shape j)))
      (replace shape shape :start1 (1+ i) :start2 i :end2 (1+ j))
      (setf (aref shape i) last))
    (setf (xecto-shape xecto) (intern-shape shape)))
  xecto)

(defun slice (xecto dimension begin &optional end step)
  ;; FIXME: -ve step
  (unless step
    (setf step 1))
  (let* ((shape  (copy-seq (xecto-shape  xecto)))
         (len    (car (aref shape dimension)))
         (stride (cdr (aref shape dimension)))
         (offset (xecto-offset xecto)))
    (unless end
      (setf end (truncate (- len begin) step)))
    (incf offset (* begin stride))
    (setf (aref shape dimension) (cons end (* step stride)))
    (setf (xecto-shape xecto)  (intern-shape shape)
          (xecto-offset xecto) offset)
    xecto))

(defun remove-index (vector index)
  (remove-if (constantly t) vector :start index :count 1))

(defun select (xecto dimension &optional value)
  (unless value
    (setf value 0))
  (let* ((shape  (copy-seq (xecto-shape xecto)))
         (offset (xecto-offset xecto)))
    (destructuring-bind (dim . stride) (aref shape dimension)
      (assert (< value dim))
      (setf (xecto-shape  xecto) (intern-shape
                                  (remove-index shape dimension))
            (xecto-offset xecto) (+ offset
                                    (* stride value)))))
  xecto)

(defun replicate (xecto &rest dimensions)
  (declare (dynamic-extent dimensions))
  (setf (xecto-shape xecto) (intern-shape
                             (concatenate 'simple-vector
                                          (mapcar (lambda (dim)
                                                    (cons dim 0))
                                                  dimensions)
                                          (xecto-shape xecto))))
  xecto)

(defun extend-shape-or-die (result-shape shape)
  (declare (type shape result-shape shape))
  (when (eql result-shape shape)
    (return-from extend-shape-or-die shape))
  (assert (every (lambda (x y)
                   (= (car x) (car y)))
                 result-shape
                 shape))
  (when (= (length result-shape) (length shape))
    (return-from extend-shape-or-die shape))
  (let ((new-shape (make-array (length result-shape))))
    (replace new-shape shape)
    (loop for i from (length shape) below (length new-shape)
          for (dim) across result-shape
          do (setf (aref new-shape i) (cons dim 0)))
    (intern-shape new-shape)))
