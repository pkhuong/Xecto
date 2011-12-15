(defpackage "XECTO-LOOP-NEST"
  (:use "CL")
  (:shadow "OPTIMIZE")
  (:export "OPTIMIZE"))

(in-package "XECTO-LOOP-NEST")

(deftype index ()
  '(and unsigned-byte fixnum))

(defun remove-index (vector index)
  (remove-if (constantly t) vector :start index :count 1))

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

(defun transpose-shapes (offsets shapes)
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

(defun optimize (offset-and-shape &rest offsets-and-shapes)
  (let* ((data    (cons offset-and-shape offsets-and-shapes))
         (offsets (map '(simple-array index 1) #'car data))
         (shapes  (map 'simple-vector #'cdr data))
         (pattern (transpose-shapes offsets shapes)))
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
