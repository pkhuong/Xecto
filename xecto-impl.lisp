(defpackage "XECTO-IMPL"
  (:use "CL" "SB-EXT" "SB-THREAD"))

;;; Internal primitive xecto stuff

(in-package "XECTO-IMPL")

;; shape representation

(deftype index ()
  '(and unsigned-byte fixnum))

(deftype shape (&optional rank)
  `(simple-array (cons index fixnum)
                 (,rank)))

(defglobal **shape-table-lock** (make-mutex :name "SHAPE TABLE LOCK"))
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
    (values stride (intern-shape canon))))

(defun make-xecto (dimensions &key initial-element)
  (multiple-value-bind (size shape)
      (size-and-shape dimensions)
    (let ((xecto (%make-xecto shape (vector-future:make size '() #()))))
      (set-finalizer xecto)
      (when initial-element
        (future:wait (xecto-data xecto) :done)
        (fill (vector-future:data (xecto-data xecto)) (float initial-element 1d0)))
      xecto)))

(defun wait (xecto &rest condition)
  (values xecto (apply 'future:wait (xecto-data xecto) condition)))

(defun copy-xecto (xecto &key shape offset)
  (let ((new (%make-xecto (or shape
                              (xecto-shape xecto))
                          (xecto-data xecto)
                          (or offset
                              (xecto-offset xecto)))))
    (vector-future:retain (xecto-data new))
    (set-finalizer new)))
