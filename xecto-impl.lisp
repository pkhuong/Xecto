(require 'sb-queue)
(load "/Users/pkhuong/xecto/thread-pool.lisp")
(load "/Users/pkhuong/xecto/futures.lisp")
(load "/Users/pkhuong/xecto/parallel-futures.lisp")
(load "/Users/pkhuong/xecto/vector-futures.lisp")

(defpackage "XECTO-IMPL"
  (:use "CL" "SB-EXT" "SB-THREAD"))

(in-package "XECTO-IMPL")

(deftype index ()
  '(and unsigned-byte fixnum))

(deftype shape (&optional rank)
  `(simple-array (cons index fixnum)
                 (,rank)))

(defglobal **shape-table-lock** (make-mutex :name "**SHAPE-TABLE-LOCK**"))
(defglobal **shape-table** (make-hash-table :test #'equalp
                                            :weakness :key-and-value))

(declaim (ftype (function (shape) (values shape &optional)) intern-shape))
(defun intern-shape (shape)
  (declare (type shape shape))
  (with-mutex (**shape-table-lock**)
    (or (gethash shape **shape-table**)
        (setf (gethash shape **shape-table**)
              shape))))

(defstruct (xecto
            (:constructor %make-xecto (%shape %data
                                       &optional (offset 0)
                                       &aux (shape  (intern-shape %shape))
                                            (handle (list %data))))
            (:copier %copy-xecto))
  (shape  nil :type shape)
  (offset nil :type index)
  (%data  nil :type vector-future:vector-future)
  (handle nil :type (cons vector-future:vector-future null)))

(defun xecto-data (xecto)
  (declare (type xecto xecto))
  (xecto-%data xecto))

(defun (setf xecto-data) (data xecto)
  (declare (type vector-future:vector-future data)
           (type xecto xecto))
  (vector-future:retain data)
  (vector-future:release (xecto-%data xecto))
  (setf (xecto-%data xecto)        data
        (car (xecto-handle xecto)) data)
  data)

(defun set-finalizer (xecto)
  (finalize xecto (let ((handle (xecto-handle xecto)))
                    (lambda ()
                      (vector-future:release (car handle))))))

(defun size-and-shape (dimensions)
  (unless (listp dimensions)
    (setf dimensions (list dimensions)))
  (let* ((rdim  (reverse dimensions))
         (shape (make-array (length rdim)))
         (len   (length shape))
         (stride 1))
    (loop for i upfrom 1
          for dim in rdim
          do (assert (typep dim 'index))
             (setf (aref shape (- len i)) (cons dim stride)
                   stride                 (* stride dim)))
    (values stride shape)))

(defun %canonical-size-and-shape (shape)
  (declare (type shape shape))
  (let* ((len   (length shape))
         (canon (make-array len))
         (stride 1))
    (loop for i upfrom 1 upto len
          for dim = (car (aref shape (- len i)))
          do (assert (typep dim 'index))
             (setf (aref canon (- len i)) (cons dim stride)
                   stride                 (* stride dim)))
    (values stride canon)))

(defun make-xecto (dimensions &key initial-element)
  (multiple-value-bind (size shape)
      (size-and-shape dimensions)
    (let ((xecto (%make-xecto shape (vector-future:make size '()))))
      (set-finalizer xecto)
      (when initial-element
        (vector-future:wait (xecto-data xecto) :done)
        (fill (vector-future:data (xecto-data xecto)) (float initial-element 1d0)))
      xecto)))

(defun wait (xecto &rest condition)
  (values xecto (apply 'vector-future:wait (xecto-data xecto) condition)))

(defun copy-xecto (xecto &key shape offset)
  (let ((new (%make-xecto (or shape
                              (xecto-shape xecto))
                          (xecto-data xecto)
                          (or offset
                              (xecto-offset xecto)))))
    (vector-future:retain (xecto-data new))
    (set-finalizer new)))

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
  (let* ((shape     (copy-seq (xecto-shape xecto)))
         (offset    (xecto-offset xecto)))
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
             (apply 'optimize-pattern
                    (cons 0 r-shape)
                    (mapcar (lambda (xecto shape)
                              (cons (xecto-offset xecto) shape))
                            args shapes))
             args))))

(defvar *max-inner-loop-count* (ash 1 16))

(defun compute-map-tasks (function pattern &rest arguments)
  (let ((tasks '())
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
                                (push
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
    (nreverse tasks)))

(defun execute-submap (destination function offsets loop arguments)
  (declare (type vector-future:vector-future destination)
           (type (simple-array index 1) offsets)
           (type (cons index (simple-array fixnum 1)) loop)
           (type (simple-array vector-future:vector-future 1)
                 arguments))
  (let ((data    (make-array (1+ (length arguments))))
        (offsets (copy-seq offsets)))
    (setf (aref data 0) (vector-future:data destination))
    (loop for i from 1 below (length data) do
      (setf (aref data i) (vector-future:data (aref arguments (1- i)))))
    (destructuring-bind (repeat . strides) loop
      (loop for i below repeat do
            (setf (aref (aref data 0) (aref offsets 0))
                  (apply function
                         (loop for j from 1 below (length data)
                               collect (aref (aref data j) (aref offsets j)))))
            (map-into offsets #'+ offsets strides)))))

(defun execute-map (fun r-size r-shape
                    pattern
                    &rest args)
  (let* ((tasks (apply 'compute-map-tasks fun pattern args))
         (data (apply 'vector-future:make
                      r-size
                      (mapcar #'xecto-data args)
                      tasks)))
    (%make-xecto r-shape data)))

(defun shapes-compatible-p (shapes)
  (let ((shape (aref shapes 0)))
    (loop for i from 1 below (length shapes)
          for other = (aref shapes i)
          always (every (lambda (x y)
                          (= (car x) (car y)))
                        shape other))))

(defun lex-compare (x y)
  (map nil (lambda (x y)
             (let ((x (abs x))
                   (y (abs y)))
               (cond ((< x y) (return-from lex-compare -1))
                     ((> x y) (return-from lex-compare  1)))))
       x y)
  0)

(defun merge-shapes (offsets shapes)
  (declare (type (simple-array index 1) offsets)
           (type simple-vector shapes))
  (assert (shapes-compatible-p shapes))
  (let* ((dimensions (map 'simple-vector #'car (aref shapes 0)))
         (pattern    (make-array (length dimensions)))
         (n          (length shapes)))
    (dotimes (i (length dimensions) pattern)
      (let ((strides (make-array n :element-type 'fixnum))
            (count   (aref dimensions i)))
        (dotimes (j n)
          (setf (aref strides j)
                (cdr (aref (aref shapes j) i))))
        (let ((nz (find 0 strides :test-not 'eql)))
          (when (and nz
                     (minusp nz))
            (map-into offsets (lambda (stride offset)
                                (+ offset (* stride count)))
                      strides offsets)
            (map-into strides #'- strides)))
        (setf (aref pattern i) (cons count strides))))))

(defun merge-pattern-1 (pattern)
  (declare (type simple-vector pattern))
  (let ((len (length pattern)))
    (loop
      for i from (1- len) downto 0
      for (i-count . i-strides) = (aref pattern i)
      do (loop
           for j from (1- i) downto 0
           for (j-count . j-strides) = (aref pattern j)
           do (when (every (lambda (i-stride j-stride)
                             (= (* i-stride i-count) j-stride))
                           i-strides j-strides)
                (setf (car (aref pattern i)) (* i-count j-count))
                (return-from merge-pattern-1 (remove-index pattern j)))))))

(defun merge-pattern (pattern)
  (declare (type simple-vector pattern))
  (loop
    (let ((new-pattern (merge-pattern-1 pattern)))
      (if new-pattern
          (setf pattern new-pattern)
          (return pattern)))))

(defun optimize-pattern (offset-and-shape &rest offsets-and-shapes)
  (let* ((data    (cons offset-and-shape offsets-and-shapes))
         (offsets (map '(simple-array index 1) #'car data))
         (shapes  (map 'simple-vector #'cdr data))
         (pattern (merge-shapes offsets shapes)))
    (sort pattern (lambda (x y)
                    (ecase (lex-compare (cdr x) (cdr y))
                      (-1 nil)
                      ( 0 (< (car x) (car y)))
                      ( 1 t))))
    (cons offsets
          (if (zerop (length pattern))
              (make-array 1 :initial-element
                          (cons 1 (make-array (length data)
                                              :element-type 'fixnum
                                              :initial-element 0)))
              (merge-pattern pattern)))))
