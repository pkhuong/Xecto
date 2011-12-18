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
    (let ((xecto (%make-xecto shape
                              (vector-future:make size
                                                  '()
                                                  (if initial-element
                                                      (vector (lambda (data)
                                                                (fill (vector-future:data data)
                                                                      (float initial-element 1d0))))
                                                      #())))))
      (set-finalizer xecto)
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

#||
(gc :full t)
(setf *print-circle* t *print-length* 20)
(defvar xx)
(defvar yy)
(sb-thread:join-thread
 (sb-thread:make-thread
  (lambda ()
    (parallel-future:with-context (11)
      (let ()
        #+nil ((xx (make-xecto '(16384 16384) :initial-element 1))
           (yy (transpose (make-xecto '(16384 16384) :initial-element 5) 0 1)))
        (setf xx (make-xecto '(16384 16384) :initial-element 1)
              yy (transpose (make-xecto '(16384 16384) :initial-element 5) 0 1))
        (time (let ((x (map-xecto #'+ xx yy))
                    (y (scan-xecto #'+ xx)))
                (wait (reduce-xecto #'+ (reduce-xecto #'+ (map-xecto #'+ x y)))
                      :done)))
        (setf *print-length* 20 xx nil yy nil))
      parallel-future:*context*))))

Evaluation took:
  104.652 seconds of real time
  104.462529 seconds of total run time (101.230327 user, 3.232202 system)
  [ Run times consist of 0.248 seconds GC time, and 104.215 seconds non-GC time. ]
  99.82% CPU

Evaluation took:
  49.085 seconds of real time
  97.618102 seconds of total run time (94.157885 user, 3.460217 system)
  [ Run times consist of 0.256 seconds GC time, and 97.363 seconds non-GC time. ]
  198.88% CPU

Evaluation took:
  26.356 seconds of real time
  103.402463 seconds of total run time (100.010251 user, 3.392212 system)
  [ Run times consist of 0.404 seconds GC time, and 102.999 seconds non-GC time. ]
  392.33% CPU

Evaluation took:
  14.404 seconds of real time
  108.718794 seconds of total run time (105.314582 user, 3.404212 system)
  [ Run times consist of 0.612 seconds GC time, and 108.107 seconds non-GC time. ]
  754.78% CPU

Evaluation took:
  11.439 seconds of real time
  110.318895 seconds of total run time (106.422651 user, 3.896244 system)
  [ Run times consist of 0.896 seconds GC time, and 109.423 seconds non-GC time. ]
  964.41% CPU
||#
