;;;; Bulk operations on multi-dimensional arrays
;;
;; Goal: exploit both SIMD and thread-level parallelism
;;
;; Strategy: evaluate expression DAGs naively, but make
;;  each operation bulky enough to be executed efficiently.
;;
;;  No loop fusion. Instead, chunk up work units to work on
;;  small working sets at a time.
;;
;;
;; Pieces (bottom up)
;;
;; Dataflow work queue
;; Bulk operations on shaped vectors
;; Lazy expression graph
;; Struct of Array representation
;;
;; (irregular array shapes)


;;; Dataflow work queue
;;
;; Not Implemented (:
;;
;; Basic version:
;;
;; - FIFO for ready units (bounded circular buffer)
;; - Each unit has ref to waiting unit
;; - Scan set of waiting units to find ready ones
;;
;; Add locality-awareness
;;
;; - Each worker thread has its own sub *stack*
;;  -> Hash to distribute units to workers,
;;  -> Steal from bottom
;; - When unit completes, try to acquire newly ready units and push
;;   to local stack.
;; - If there's a lot of newly ready units, distribute according to hash
;;
;; - Hash: middle bits of written range.

;;; Bulk operations on shaped vectors
;;
;; Prototyped below.
;;
;;
;;

;;; Keep graph implicit.
;;
;; Each backing vector is either a vector or a promise.
;;
;; Promise:
;;  - vector not yet allocated
;;  - Each actual work unit annotated with write range.
;;  - Work units depend on promise or work unit.
;;     -> must add self to dependent list; just CAS.



;;; SoA structured data
;;
;;

;; Next step
;;
;; Ref-counted CoW
;; Flatten representation operation.
;; More types
;; Laziness
;; Expression graph rewriting (only trivial copy-elim stuff)
;; Work queue
;;  - recursive work queue for reduce/scan
;;  - enable pipelining by tracking first/last index written/read
;;
;; Inner loop: call dispatch once and find right implementation
;;  -> special case common strides (1, 0, same)
;;  -> reduce/scan: transpose evaluation order
;;  -> parallelise really long inner loops or wide middle loops

;; Then: safety checks.
;; Bulk sorting, gather/scatter, Sort-indices

;; SoA
;;
;; TODO: make sure all the selection/reshaping/permutation operations
;;  implicitly map over all slots as appropriate

(defstruct (xstruct
            (:constructor %make-xstruct (&optional (slots (make-hash-table)))))
  (slots nil :type hash-table :read-only t))

(defun make-xstruct (&rest name-and-content)
  (let ((slots (make-hash-table)))
    (loop for (slot content) on name-and-content by #'cddr
          do
             (assert (typep content '(or real xarray)))
             (setf (gethash slot slots)
                   (if (realp content)
                       (singleton content)
                       content)))))

(defun xslot (xstruct slot)
  (or (gethash slot (xstruct-slots xstruct))
      (error "Unknown slot ~S" slot)))

(defun (setf xslot) (value xstruct slot)
  (assert (typep value '(or real xarray)))
  (setf (gethash slot (xstruct-slots xstruct))
        (if (realp value)
            (singleton value)
            value))
  value)

;; Array as linear vector + affine transform

(defvar *shape-table* (make-hash-table :test 'equalp
                                       :weakness :key-and-value))
(defun intern-shape (shape)
  (or (gethash shape *shape-table*)
      (setf (gethash shape *shape-table*)
            (make-array (length shape)
                        :element-type '(and unsigned-byte fixnum)
                        :initial-contents shape))))

(defvar *transform-table* (make-hash-table :test 'equalp
                                           :weakness :key-and-value))
(defun intern-transform (transform)
  (or (gethash transform *transform-table*)
      (setf (gethash transform *transform-table*)
            (make-array (length transform)
                        :element-type 'fixnum
                        :initial-contents transform))))

(defstruct (xarray
            (:constructor make-xarray
                (%shape %transform offset backing
                 &aux
                   (shape (intern-shape %shape))
                   (transform (intern-transform %transform)))))
  (shape nil :type (simple-array (and unsigned-byte fixnum) 1) :read-only t)
  (transform nil :type (simple-array fixnum 1) :read-only t)
  (offset 0 :read-only t)
  (backing nil :type (simple-array double-float 1) :read-only t))

(defun strides (shape)
  (let ((acc     1)
        (strides (make-array (length shape))))
    (loop for i from (1- (length shape)) downto 0
          for size = (aref shape i)
          do (setf (aref strides i) acc
                   acc (* acc size))
          finally (return (values strides acc)))))

(defun %xarray (shape)
  (declare (type vector shape))
  (multiple-value-bind (strides total-size)
      (strides shape)
    (make-xarray shape strides 0
                 (make-array total-size
                             :element-type 'double-float))))

(defun xarray (&rest shape)
  (declare (dynamic-extent shape))
  (%xarray (coerce shape 'simple-vector)))

(defun xarray-from-vector (vector)
  (let ((result (xarray (length vector))))
    (map-into (xarray-backing result)
              (lambda (x)
                (float x 1d0))
              vector)
    result))

(defun singleton (x)
  (let ((result (xarray)))
    (setf (aref (xarray-backing result) 0) (float x 1d0))
    result))

(defun normalize (x)
  (etypecase x
    (real (singleton x))
    (xarray x)))

(define-modify-macro %normalizef () normalize)

(defmacro normalizef (&rest places)
  `(progn
     ,@(mapcar (lambda (x)
                 `(%normalizef ,x))
               places)))

(defun denormalize (x)
  (if (and (xarray-p x)
           (zerop (length (xarray-shape x))))
      (aref (xarray-backing x)
            (xarray-offset x))
      x))

(defun iota (n)
  (let ((result (xarray n))
        (index  0d0))
    (declare (type double-float index))
    (map-into (xarray-backing result)
              (lambda ()
                (incf index)))
    result))

(defun shape (xarray &optional dimension)
  (if dimension
      (aref (xarray-shape xarray) dimension)
      (xarray-shape xarray)))

(defun reshape (xarray shape &optional strides)
  (unless strides
    (setf strides (strides shape)))
  (make-xarray shape
               strides
               (xarray-offset xarray)
               (xarray-backing xarray)))

;; (defun stack-transform (xarray )) <- linear transform.

(defun transpose (xarray &rest indices)
  (declare (dynamic-extent indices))
  (let ((shape         (xarray-shape xarray))
        (new-shape     (make-array (length indices)))
        (transform     (xarray-transform xarray))
        (new-transform (make-array (length indices))))
    (loop for i upfrom 0
          for index in indices
          do (setf (aref new-shape i)     (aref shape index)
                   (aref new-transform i) (aref transform index)))
    (make-xarray new-shape
                 new-transform
                 (xarray-offset xarray)
                 (xarray-backing xarray))))

(defun slice (xarray dimension begin &optional end step)
  ;; FIXME: -ve step
  (unless step
    (setf step 1))
  (let* ((shape  (copy-seq (xarray-shape xarray)))
         (transform (copy-seq (xarray-transform xarray)))
         (stride (aref transform dimension))
         (offset (xarray-offset xarray)))
    (unless end
      (setf end (truncate (- (aref shape dimension) begin)
                          step)))
    (incf offset (* begin stride))
    (setf (aref shape dimension) end
          (aref transform dimension) (* step stride))
    (make-xarray shape  transform
                 offset
                 (xarray-backing xarray))))

(defun remove-index (vector index)
  (remove-if (constantly t) vector :start index :count 1))

(defun select (xarray dimension &optional value)
  (unless value
    (setf value 0))
  (let ((shape     (xarray-shape xarray))
        (transform (xarray-transform xarray))
        (offset    (xarray-offset xarray)))
    (make-xarray (remove-index shape dimension)
                 (remove-index transform dimension)
                 (+ offset (* value (aref transform dimension)))
                 (xarray-backing xarray))))

(defun vcons (x vector)
  (let ((r (make-array (1+ (length vector)))))
    (setf (aref r 0) x)
    (replace r vector :start1 1)
    r))

(defun replicate (xarray &rest counts)
  (declare (dynamic-extent counts))
  (reshape xarray
           (concatenate 'simple-vector counts (xarray-shape xarray))
           (concatenate 'simple-vector
                        (make-array (length counts) :initial-element 0)
                        (xarray-transform xarray))))

;; actual operations

(defun extend-shape-or-die (target-shape xarray)
  (let ((shape (xarray-shape xarray)))
    (assert (<= (length shape) (length target-shape)))
    (loop for i from (1- (length target-shape)) downto 0
          for j from (1- (length shape))        downto 0
          do (assert (= (aref target-shape i)
                        (aref shape j))))
    (apply 'replicate xarray
           (coerce (subseq target-shape 0 (- (length target-shape)
                                             (length shape)))
                   'list))))

;; TODO:
;; Inline all of this
;;  so that execute-*-into can have inline caches re pattern.

(defun args-parameters (args)
  (mapcan (lambda (arg)
            (list (xarray-shape arg)
                  (xarray-transform arg)))
          args))

(define-compiler-macro optimize-pattern (x y &rest args)
  `((lambda (args)
      (let* ((box  (load-time-value (list nil)))
             (cache (car box))
             (params (args-parameters args)))
        (if (and cache
                 (every #'eql params (car cache)))
            (cdr cache)
            (let ((result (%optimize-pattern args)))
              (setf (car box)
                    (cons params result))
              result))))
    (list ,x ,y ,@args)))

(declaim (inline xscan-into scan
                 xreduce-into xreduce))

(defun xmap-into (dst op x &rest xs)
  (declare (dynamic-extent xs))
  (normalizef dst)
  (let ((xs (cons x xs)))
    (let ((dst-shape (xarray-shape dst)))
      (map-into xs (lambda (x)
                     (extend-shape-or-die dst-shape (normalize x)))
                xs)
      (apply 'execute-map-into op
             (apply 'optimize-pattern dst xs)
             dst xs)
      (denormalize dst))))

(define-compiler-macro xmap-into (dst op x &rest xs)
  (let ((names (cons 'x (loop for x in xs collect (gensym "X")))))
    `((lambda (dst op ,@names)
        (normalizef dst)
        (let ((dst-shape (xarray-shape dst)))
          ,@(loop for x in names
                  collect `(setf ,x (extend-shape-or-die dst-shape (normalize ,x))))
          (execute-map-into op
                            (optimize-pattern dst ,@names)
                            dst ,@names)
          (denormalize dst)))
      ,dst ,op ,x ,@xs)))

(defun build-dst-for (x &rest xs)
  (let ((best-shape (xarray-shape x)))
    (dolist (x xs (%xarray best-shape))
      (let ((shape (xarray-shape x)))
        (when (> (length shape) (length best-shape))
          (setf best-shape shape))))))

(defun xmap (op x &rest xs)
  (let ((xs (cons x xs)))
    (apply 'xmap-into
           (apply 'build-dst-for (mapcar 'normalize xs))
           op xs)))

(define-compiler-macro xmap (op x &rest xs)
  (let ((names (cons 'x (loop for x in xs collect (gensym "X")))))
    `((lambda (op ,@names)
        (normalizef ,@names)
        (xmap-into (build-dst-for ,@names)
                   op ,@names))
      ,op ,x ,@xs)))

(defun xscan-into (dst op x)
  (normalizef dst x)
  (let* ((dst-shape (xarray-shape dst))
         (x         (extend-shape-or-die dst-shape x))
         (spine-dim (aref dst-shape 0))
         (dst-stride (aref (xarray-transform dst) 0))
         (x-stride   (aref (xarray-transform x)   0))
         (dst       (select dst 0 0))
         (x         (select x   0 0)))
    ;; dst-stride = 0 -> reduce...
    (execute-scan-into op (optimize-pattern dst x)
                       dst-stride x-stride spine-dim
                       dst x)
    (denormalize dst)))

(defun xscan (op x)
  (normalizef x)
  (xscan-into (%xarray (xarray-shape x))
              op x))


(defun shape= (x y)
  (let ((sx (xarray-shape x))
        (sy (xarray-shape y)))
    (and (= (length sx) (length sy))
         (every #'= sx sy))))

(defun xreduce-into (dst op x)
  (normalizef dst x)
  (let* ((src-shape (xarray-shape x))
         (spine-dim (aref src-shape 0))
         (spine-stride (aref (xarray-transform x) 0))
         (x         (extend-shape-or-die (xarray-shape dst)
                                         (select x 0 0))))
    (execute-reduce-into op
                         (optimize-pattern dst x)
                         spine-stride spine-dim
                         dst x)
    (denormalize dst)))

(defun xreduce (op x)
  (normalizef x)
  (xreduce-into (%xarray (subseq (xarray-shape x) 1))
                op x))

(defvar *map-into-routines* (make-hash-table :test #'equal))

(defmacro def-map-into/3 (op &optional (fun op))
  (let ((name (intern (format nil "%EXECUTE-MAP-INTO/3-~A" op))))
    `(progn
       (defun ,name (vecs offsets count increment)
         (declare (type (simple-vector 3) vecs increment)
                  (type (simple-array (and unsigned-byte fixnum) (3)) offsets)
                  (type (and unsigned-byte fixnum) count))
         (let ((v0 (aref vecs 0))
               (v1 (aref vecs 1))
               (v2 (aref vecs 2))
               (i0 (aref offsets 0))
               (i1 (aref offsets 1))
               (i2 (aref offsets 2))
               (d0 (aref increment 0))
               (d1 (aref increment 1))
               (d2 (aref increment 2)))
           (declare (type (simple-array double-float 1) v0 v1 v2)
                    (type (and unsigned-byte fixnum) i0 i1 i2)
                    (type fixnum d0 d1 d2)
                    (optimize speed (safety 0)))
           (loop repeat count
                 do
                    (setf (aref v0 i0) (,fun (aref v1 i1) (aref v2 i2)))
                    (incf i0 d0)
                    (incf i1 d1)
                    (incf i2 d2))))
       (setf (gethash '(,op . 3) *map-into-routines*)
             ',name)
       ,(let ((name  (intern (format nil "V~A" op)))
              (name! (intern (format nil "V~A!" op))))
          `(progn
             (declaim (inline ,name ,name!))
             (defun ,name (x y)
                 (xmap ',op x y))
             (defun ,name! (dst x y)
               (xmap-into dst ',op x y)))))))

(def-map-into/3 +)
(def-map-into/3 minus -)
(def-map-into/3 *)
(def-map-into/3 /)

(defmacro def-map-into/2 (op &optional (fun op))
  (let ((name (intern (format nil "%EXECUTE-MAP-INTO/2-~A" op))))
    `(progn
       (defun ,name (vecs offsets count increment)
         (declare (type (simple-vector 2) vecs increment)
                  (type (simple-array (and unsigned-byte fixnum) (2)) offsets)
                  (type (and unsigned-byte fixnum) count))
         (let ((v0 (aref vecs 0))
               (v1 (aref vecs 1))
               (i0 (aref offsets 0))
               (i1 (aref offsets 1))
               (d0 (aref increment 0))
               (d1 (aref increment 1)))
           (declare (type (simple-array double-float 1) v0 v1)
                    (type (and unsigned-byte fixnum) i0 i1)
                    (type fixnum d0 d1)
                    (optimize speed (safety 0)))
           (loop repeat count
                 do
                    (setf (aref v0 i0) (,fun (aref v1 i1)))
                    (incf i0 d0)
                    (incf i1 d1))))
       (setf (gethash '(,op . 2) *map-into-routines*)
             ',name)
       ,(let ((name  (intern (format nil "V~A" op)))
              (name! (intern (format nil "V~A!" op))))
          `(progn
             (declaim (inline ,name ,name!))
             (defun ,name (x)
               (xmap ',op x))
             (defun ,name! (dst x)
               (xmap-into dst ',op x)))))))

(def-map-into/2 sqrt (lambda (x)
                       (sqrt (truly-the (double-float 0d0) x))))
(def-map-into/2 neg (lambda (x)
                      (- x)))
(def-map-into/2 abs)

(declaim (inline v- v-!))
(defun v- (x &optional y)
  (if y
      (vminus x y)
      (vneg x)))

(defun v-! (dst x &optional y)
  (if y
      (vminus! dst x y)
      (vneg! dst x)))

(defun find-map-into-routine (op arity)
  (symbol-function (or (gethash (cons op arity) *map-into-routines*)
                       (error "Unknown map-into routine ~S/~A" op arity))))

#+nil
(defun %execute-map-into (op vecs offsets count increment)
  (declare (type simple-vector vecs increment)
           (type (simple-array (and unsigned-byte fixnum) 1) offsets)
           (type (and unsigned-byte fixnum) count))
  (assert (= 3 (length vecs)))
  (assert (eql '+ op))
  (let ((v0 (aref vecs 0))
        (v1 (aref vecs 1))
        (v2 (aref vecs 2))
        (i0 (aref offsets 0))
        (i1 (aref offsets 1))
        (i2 (aref offsets 2))
        (d0 (aref increment 0))
        (d1 (aref increment 1))
        (d2 (aref increment 2)))
    (declare (type (simple-array double-float 1) v0 v1 v2)
             (type (and unsigned-byte fixnum) i0 i1 i2)
             (type fixnum d0 d1 d2)
             (optimize speed (safety 0)))
    (loop repeat count
          do
             (setf (aref v0 i0) (+ (aref v1 i1) (aref v2 i2)))
             (incf i0 d0)
             (incf i1 d1)
             (incf i2 d2))))

(defun execute-map-into (op pattern &rest args)
  (declare (dynamic-extent args))
  (let* ((vectors (map 'simple-vector 'xarray-backing args))
         (n       (length args))
         (fun     (find-map-into-routine op n))
         (offsets (make-array n
                              :element-type '(and unsigned-byte fixnum)
                              :initial-contents (car pattern)))
         (loops   (cdr pattern))
         (depth   (length loops)))
    (declare (type simple-vector loops))
    (labels ((rec (i)
               (let* ((spec      (aref loops i))
                      (count     (car spec))
                      (increment (cdr spec)))
                 (if (= i (1- depth))
                     (funcall fun vectors
                              offsets
                              count
                              increment)
                     (loop repeat count
                           do (rec (1+ i))
                              (map-into offsets #'+ offsets increment))))))
      (rec 0)
      (values))))

(defvar *reduce-into-routines* (make-hash-table :test #'equal))

(defmacro def-reduce-into (op)
  (let ((name (intern (format nil "%EXECUTE-REDUCE-INTO/~A" op))))
    `(progn
       (defun ,name (stride dim
                     dst x
                     dst-offset x-offset
                     count
                     dst-inc x-inc)
         ;; switch depending on which is smaller of stride or dst-offset
         (declare (type fixnum stride)
                  (type (and unsigned-byte fixnum)
                        dim
                        dst-offset x-offset count
                        dst-inc x-inc)
                  (type (simple-array double-float 1) dst x))
         (locally (declare (optimize speed (safety 0)))
           (loop repeat count
                 do
                    (let ((x-offset x-offset)
                          (acc      0d0))
                      (declare (type (and unsigned-byte fixnum) x-offset)
                               (type double-float acc))
                      (loop repeat dim
                            do
                               (setf acc (,op acc (aref x x-offset)))
                               (incf x-offset stride))
                      (setf (aref dst dst-offset) acc))
                    (incf dst-offset dst-inc)
                    (incf x-offset   x-inc))))
       (setf (gethash ',op *reduce-into-routines*)
             ',name)
       ,(let ((name  (intern (format nil "R~A" op)))
              (name! (intern (format nil "R~A!" op))))
          `(progn
             (declaim (inline ,name ,name!))
             (defun ,name (x)
               (xreduce ',op x))
             (defun ,name! (dst x)
               (xreduce-into dst ',op x)))))))

(def-reduce-into +)
(def-reduce-into *)
(def-reduce-into min)
(def-reduce-into max)

(defun find-reduce-into-routine (op)
  (symbol-function (or (gethash op *reduce-into-routines*)
                       (error "No reduce into routine for ~S" op))))

#+nil
(defun %execute-reduce-into (op stride dim
                             dst x
                             dst-offset x-offset
                             count
                             dst-inc x-inc)
  ;; switch depending on which is smaller of stride or dst-offset
  (declare (type fixnum stride)
           (type (and unsigned-byte fixnum)
                 dim
                 dst-offset x-offset count
                 dst-inc x-inc)
           (type (simple-array double-float 1) dst x)
           (type (eql +) op))
  (locally (declare (optimize speed (safety 0)))
    (loop repeat count
          do
             (let ((x-offset x-offset)
                   (acc      0d0))
               (declare (type (and unsigned-byte fixnum) x-offset)
                        (type double-float acc))
               (loop repeat dim
                     do
                        (incf acc (aref x x-offset))
                        (incf x-offset stride))
               (setf (aref dst dst-offset) acc))
             (incf dst-offset dst-inc)
             (incf x-offset   x-inc))))

(defun execute-reduce-into (op pattern stride dim dst x)
  (let* ((offsets (car pattern))
         (loops   (cdr pattern))
         (depth   (length loops))
         (dst-offset (aref offsets 0))
         (x-offset   (aref offsets 1))
         (dst     (xarray-backing dst))
         (x       (xarray-backing x))
         (fun     (find-reduce-into-routine op)))
    (declare (type simple-vector loops)
             (type (and unsigned-byte fixnum) dst-offset x-offset))
    (labels ((rec (i)
               (let* ((spec      (aref loops i))
                      (count     (car spec))
                      (increment (cdr spec))
                      (dst-inc   (aref increment 0))
                      (x-inc     (aref increment 1)))
                 (if (= i (1- depth))
                     (funcall fun stride dim
                              dst x
                              dst-offset x-offset
                              count
                              dst-inc x-inc)
                     (loop repeat count
                           do (rec (1+ i))
                              (incf x-offset x-inc)
                              (incf dst-offset dst-inc))))))
      (rec 0)
      (values))))

(defvar *scan-into-routines* (make-hash-table :test #'equal))

(defmacro def-scan-into (op)
  (let ((name (intern (format nil "%EXECUTE-SCAN-INTO/~A" op))))
    `(progn
       (defun ,name (dst-stride x-stride dim
                     dst x
                     dst-offset x-offset
                     count
                     dst-inc x-inc)
         (declare (type fixnum dst-stride x-stride)
                  (type (and unsigned-byte fixnum)
                        dim
                        dst-offset x-offset count
                        dst-inc x-inc)
                  (type (simple-array double-float 1) dst x))
         (locally (declare (optimize speed (safety 0)))
           (loop repeat count
                 do
                    (let ((dst-offset dst-offset)
                          (x-offset   x-offset)
                          (acc        0d0))
                      (declare (type (and unsigned-byte fixnum)
                                     x-offset dst-offset)
                               (type double-float acc))
                      (loop repeat dim
                            do
                               (setf acc
                                     (setf (aref dst dst-offset)
                                           (,op acc (aref x x-offset))))
                               (incf dst-offset dst-stride)
                               (incf x-offset x-stride)))
                    (incf dst-offset dst-inc)
                    (incf x-offset   x-inc))))
       (setf (gethash ',op *scan-into-routines*)
             ',name)
       ,(let ((name  (intern (format nil "S~A" op)))
              (name! (intern (format nil "S~A!" op))))
          `(progn
             (declaim (inline ,name ,name!))
             (defun ,name (x)
               (xscan ',op x))
             (defun ,name! (dst x)
               (xscan-into dst ',op x)))))))

(def-scan-into +)
(def-scan-into *)
(def-scan-into min)
(def-scan-into max)

(defun find-scan-into-routine (op)
  (symbol-function (or (gethash op *reduce-into-routines*)
                       (error "No reduce into routine for ~S" op))))

#+nil
(defun %execute-scan-into (op dst-stride x-stride dim
                           dst x
                           dst-offset x-offset
                           count
                           dst-inc x-inc)
  (declare (type fixnum dst-stride x-stride)
           (type (and unsigned-byte fixnum)
                 dim
                 dst-offset x-offset count
                 dst-inc x-inc)
           (type (simple-array double-float 1) dst x)
           (type (eql +) op))
  (locally (declare (optimize speed (safety 0)))
    (loop repeat count
          do
             (let ((dst-offset dst-offset)
                   (x-offset   x-offset)
                   (acc        0d0))
               (declare (type (and unsigned-byte fixnum)
                              x-offset dst-offset)
                        (type double-float acc))
               (loop repeat dim
                     do
                        (incf acc (aref x x-offset))
                        (setf (aref dst dst-offset) acc)
                        (incf dst-offset dst-stride)
                        (incf x-offset x-stride)))
             (incf dst-offset dst-inc)
             (incf x-offset   x-inc))))

(defun execute-scan-into (op pattern dst-stride x-stride dim dst x)
  (let* ((fun     (find-scan-into-routine op))
         (offsets (car pattern))
         (loops   (cdr pattern))
         (depth   (length loops))
         (dst-offset (aref offsets 0))
         (x-offset   (aref offsets 1))
         (dst     (xarray-backing dst))
         (x       (xarray-backing x)))
    (declare (type simple-vector loops)
             (type (and unsigned-byte fixnum) dst-offset x-offset))
    (labels ((rec (i)
               (let* ((spec      (aref loops i))
                      (count     (car spec))
                      (increment (cdr spec))
                      (dst-inc   (aref increment 0))
                      (x-inc     (aref increment 1)))
                 (if (= i (1- depth))
                     (funcall fun dst-stride x-stride dim
                              dst x
                              dst-offset x-offset
                              count
                              dst-inc x-inc)
                     (loop repeat count
                           do (rec (1+ i))
                              (incf x-offset x-inc)
                              (incf dst-offset dst-inc))))))
      (rec 0)
      (values))))

;; machinery for optimize-pattern
;; Reorder perfect constant loop nests for locality, while
;; trying to ensure a minimal trip count for the inner loop

(defvar *iterator-object-count* 0)

(defstruct (iterator
            (:constructor make-iterator (limit &optional name)))
  (name  (incf *iterator-object-count*) :read-only t)
  (limit nil :read-only t))

(defstruct (index-seq
            (:constructor make-index-seq (offset plane)))
  (offset  0 :read-only t :type fixnum)
  (plane nil :read-only t :type simple-vector))

(defstruct (fused-index-seq
            (:constructor make-fused-index-seq (offsets planes)))
  (offsets nil :read-only t :type simple-vector)
  (planes  nil :read-only t :type simple-vector))

(defun %make-expression (offset iters strides)
  (let ((rest (loop for iter across iters
                    for stride across strides
                    do (when (minusp stride)
                         (setf stride (- stride))
                         (decf offset (* stride (cdr iter))))
                    when iter
                      collect (cons iter stride))))
    (make-index-seq offset (coerce rest 'simple-vector))))

(defun make-expression (offset &rest iter-and-stride)
  (let ((rest (loop for (iter stride) on iter-and-stride by #'cddr
                    do (when (minusp stride)
                         (setf stride (- stride))
                         (decf offset (* stride (cdr iter))))
                    collect (cons iter stride))))
    (make-index-seq offset (coerce rest 'simple-vector))))

(defun lex> (x y)
  (assert (vectorp x))
  (assert (vectorp y))
  (assert (= (length x) (length y)))
  (loop for xi across x
        for yi across y
        do (cond ((< xi yi) (return nil))
                 ((> xi yi) (return t)))
        finally (return nil)))

(defun fuse-expressions (&rest expressions)
  (let ((iters (make-hash-table))
        (transposed '())
        (offsets (map 'simple-vector #'index-seq-offset expressions))
        (count (length expressions))
        (expressions (mapcar #'index-seq-plane expressions)))
    (loop for i upfrom 0
          for expression in expressions
          do
          (loop for (iter . stride) across expression
                do
                (let ((strides
                        (or (gethash iter iters)
                            (let ((values (make-array count :initial-element 0)))
                              (push (cons iter values) transposed)
                              (setf (gethash iter iters)
                                    values)))))
                  (setf (aref strides i) stride))))
    (make-fused-index-seq
     offsets
     (sort (coerce transposed 'simple-vector)
           (lambda (x y)
             (let ((xx (cdr x))
                   (yy (cdr y)))
               (if (every #'= xx yy)
                   (> (cdar x) (cdar y))
                   (lex> xx yy))))))))

(defun %merge-iterators (strides)
  (loop for i from (1- (length strides)) downto 0
        for (iter . stride) = (aref strides i)
        for repeat = (iterator-limit iter) do
          (loop for j below i
                for (j-iter . j-stride) = (aref strides j)
                for j-repeat = (iterator-limit j-iter)
                do
                  (when (every (lambda (i j)
                                 (= (* repeat i) j))
                               stride j-stride)
                    (setf (car (aref strides i))
                          (make-iterator (* repeat j-repeat)))
                    (return-from %merge-iterators
                      (remove-index strides j))))))

(defun merge-iterators (fused)
  (let ((offsets (fused-index-seq-offsets fused))
        (strides (fused-index-seq-planes fused)))
    (loop for result = (%merge-iterators strides)
          do (if result
                 (setf strides result)
                 (return (make-fused-index-seq offsets strides))))))

(defun reorder-iterators (fused &key (goal-trip-count 32))
  (let ((strides (copy-seq (fused-index-seq-planes fused)))
        (best-idx  nil)
        (best-trip   0))
    (when (zerop (length strides))
      (let* ((offsets (fused-index-seq-offsets fused))
             (n       (length offsets)))
        (return-from
         reorder-iterators
          (make-fused-index-seq
           offsets (vector (cons (make-iterator 1)
                                 (make-array n :initial-element 0)))))))
    (loop for i upfrom 0
          for (iter) across strides
          for capped-count = (min (iterator-limit iter) goal-trip-count)
          do
             (when (>= capped-count best-trip)
               (setf best-idx  i
                     best-trip capped-count)))
    (let ((bottom (aref strides best-idx)))
      (loop for i from (1+ best-idx) below (length strides)
            do (setf (aref strides (1- i))
                     (aref strides i)))
      (setf (aref strides (1- (length strides))) bottom))
    (make-fused-index-seq (fused-index-seq-offsets fused)
                          strides)))

#||
CL-USER> (let ((i (make-iterator 4 'i))
               (j (make-iterator 3 'j))
               (k (make-iterator 20 'k)))
           (reorder-iterators
            (merge-iterators (fuse-expressions 
                              (make-expression 0 i 1 j 4 k 12)
                              (make-expression 1 i 2 j 8 k 24)))))
(#(0 1) . #(((I . 240) . #(1 2))))
||#

(defun %optimize-pattern (args)
  (format t "optimizing pattern ...~%")
  (let* ((*iterator-object-count* 0)
         (shape (xarray-shape (first args)))
         (iterators (map 'simple-vector
                         (lambda (dim)
                           (and (> dim 1)
                                (make-iterator dim)))
                         shape))
         (expressions (mapcar (lambda (arg)
                                (%make-expression (xarray-offset arg)
                                                  iterators
                                                  (xarray-transform arg)))
                              args))
         (pattern     (reorder-iterators
                       (merge-iterators
                        (apply 'fuse-expressions expressions))))
         (result      (cons (fused-index-seq-offsets pattern)
                            (map 'simple-vector
                                 (lambda (x)
                                   (cons (iterator-limit (car x))
                                         (cdr x)))
                                 (fused-index-seq-planes pattern)))))
    (format t "    ~A~%" result)
    result))

(defun optimize-pattern (x y &rest arguments)
  (%optimize-pattern (list* x y arguments)))
;; test

(defun avg (values)
  (v/ (r+ values)
      (shape values 0)))

(defun sd (values)
  (vsqrt (avg (let ((delta (v- values (avg values))))
                (v* delta delta)))))

(defun fast-sd (values)
  (declare (type (simple-array double-float 1) values)
           (optimize speed (safety 0)))
  (let ((avg 0d0))
    (declare (type double-float avg))
    (map nil (lambda (x)
               (incf avg x))
         values)
    (setf avg (/ avg (length values)))
    (let ((deltas (make-array (length values)
                              :element-type 'double-float)))
      (declare (type (simple-array double-float 1) deltas))
      (map-into deltas
                (lambda (x)
                  (- x avg))
                values)
      (loop for i below (length deltas)
            do (setf (aref deltas i)
                     (expt (aref deltas i) 2)))
      (let ((sum 0d0))
        (declare (type double-float sum))
        (map nil (lambda (x)
                   (incf sum x))
             deltas)
        (sqrt (truly-the (double-float 0d0)
                         (/ sum (length values))))))))
(defun %sd (values)
  (vsqrt (v- (avg (v* values values))
             (let ((avg (avg values)))
               (v* avg avg)))))
