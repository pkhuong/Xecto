(defpackage "PARALLEL"
  (:use)
  (:export "PROMISE" "PROMISE-VALUE" "PROMISE-VALUE*" "LET"
           "FUTURE" "FUTURE-VALUE" "FUTURE-VALUE*" "BIND"
           "DOTIMES" "MAP" "REDUCE"
           "MAP-GROUP-REDUCE"))

(defpackage "PARALLEL-IMPL"
  (:use "CL" "SB-EXT")
  (:import-from "PARALLEL" "PROMISE" "PROMISE-VALUE" "PROMISE-VALUE*"
                "FUTURE" "FUTURE-VALUE"))

(in-package "PARALLEL-IMPL")

(deftype status ()
  `(member :waiting :done))

(defstruct (parallel:promise
            (:constructor make-promise (function))
            (:include work-stack:task))
  %values
  (%status :waiting :type (or status promise-slow-status)))

(status:define-status-type promise-slow-status
    (:fast-type promise
     :status-type status
     :default-status :waiting
     :final-states (:done))
    promise-%status
    promise-status
    %promise-wait
    %promise-upgrade)

(defun parallel:promise (thunk &rest args)
  (let ((promise
          (make-promise (lambda (promise)
                          (declare (type promise promise))
                          (setf (promise-%values promise)
                                (multiple-value-list (apply thunk args)))
                          (%promise-upgrade promise :done :waiting)))))
    (work-queue:push-self promise (work-queue:current-queue
                                   parallel-future:*context*))
    promise))

(defun parallel:promise-value (promise)
  (declare (type promise promise))
  (when (work-queue:worker-id)
    (work-queue:progress-until
     (lambda ()
       (eql :done (promise-status promise)))))
  (%promise-wait promise :done)
  (values-list (promise-%values promise)))

(defun parallel:promise-value* (promise)
  (unless (promise-p promise)
    (return-from parallel:promise-value* promise))
  (multiple-value-call (lambda (&optional (value nil value-p) &rest args)
                         (cond ((promise-p value)
                                (parallel:promise-value* value))
                               (value-p
                                (multiple-value-call #'values
                                  value (values-list args)))
                               (t
                                (values))))
    (parallel:promise-value promise)))

(defmacro parallel:let ((&rest bindings) &body body)
  (let* ((parallelp   t)
         (names       '())
         (values      '())
         (temporaries (loop for (name value) in bindings
                            if (eql name :parallel)
                              do (setf parallelp value)
                            else collect
                            (progn
                              (push name names)
                              (push value values)
                              `(,(gensym "PROMISE") (promise
                                                     (lambda ()
                                                       ,value))))))
         (function   (gensym "PARALLEL-LET-FUNCTION")))
    (setf names  (nreverse names)
          values (nreverse values))
    `(flet ((,function (,@names)
              ,@body))
       (if ,parallelp
           (let ,temporaries
             (,function ,@(loop for (temp) in temporaries
                                collect `(promise-value ,temp))))
           (,function ,@values)))))

(defstruct (parallel:future
            (:include parallel-future:future))
  %values)

(defun call-with-future-values (function futures)
  (declare (type simple-vector futures))
  (apply function (map 'list (lambda (x)
                               (if (future-p x)
                                   (future-value x)
                                   x))
                       futures)))

(defun parallel:future (dependencies callback &key subtasks cleanup)
  (declare (type simple-vector dependencies)
           (type (or null simple-vector) subtasks))
  (let ((future (parallel-future:make
                 (remove-if-not #'future-p dependencies)
                 (lambda (self)
                   (setf (future-%values self)
                         (multiple-value-list
                          (call-with-future-values
                           callback dependencies))))
                 (or subtasks #())
                 (and cleanup
                      (lambda (self)
                        (setf (future-%values self)
                              (multiple-value-list
                               (call-with-future-values
                                cleanup dependencies)))))
                 #'make-future)))
    (work-queue:push-self future (work-queue:current-queue
                                  parallel-future:*context*))
    future))

(defun parallel:future-value (future)
  (declare (type future future))
  (when (work-queue:worker-id)
    (work-queue:progress-until (lambda ()
                                 (eql (future:status future) :done))))
  (future:wait future :done)
  (values-list (future-%values future)))

(defun parallel:future-value* (future)
  (unless (future-p future)
    (return-from parallel:future-value* future))
  (loop
    (multiple-value-call
        (lambda (&optional (value nil value-p) &rest values)
          (cond ((future-p value)
                 (setf future value))
                (value-p
                 (return (multiple-value-call #'values
                           value (values-list values))))
                (t
                 (return (values)))))
      (parallel:future-value future))))

(defmacro parallel:bind ((&rest bindings)
                         &body body)
  (let ((wait nil))
    (when (eql :wait (car body))
      (setf wait t)
      (pop body))
    `(,(if wait 'future-value 'identity)
      (future (vector ,@(mapcar #'second bindings))
              (lambda ,(mapcar #'first bindings)
                ,@body)))))

(defun %call-n-times (count function cleanup)
  (let ((future
          (parallel-future:make
           #()
           nil
           (make-array count :initial-element 0)
           (and cleanup
                (lambda (self)
                  (setf (future-%values self)
                        (multiple-value-list (funcall cleanup)))))
           #'make-future
           :%values '(nil)
           :subtask-function (lambda (subtask self index)
                               (declare (ignore subtask self))
                               (funcall function index)))))
    (work-queue:push-self future (work-queue:current-queue
                                  parallel-future:*context*))
    future))

(defun call-n-times (count function aggregate-function &optional cleanup)
  (let* ((worker-count (or (work-queue:worker-count
                            (work-queue:current-queue
                             parallel-future:*context*))
                           (error "No current queue")))
         (max          (expt worker-count 2)))
    (if (<= count max)
        (%call-n-times count function cleanup)
        (let ((step   (truncate count max)))
          (%call-n-times (ceiling count step)
                         (lambda (i)
                           (let* ((begin (* i step))
                                  (end   (min (+ begin step) count)))
                             (funcall aggregate-function begin end)))
                         cleanup)))))

(defmacro parallel:dotimes ((var count &optional result) &body body)
  (let ((begin (gensym "BEGIN"))
        (end   (gensym "END"))
        (i     (gensym "I"))
        (wait  nil)
        (tid   (gensym "TID")))
    (when (eql (car body) :wait)
      (setf wait t)
      (pop body))
    `(,(if wait 'future-value 'identity)
      (call-n-times ,count
                    (lambda (,var)
                      ,@body)
                    (lambda (,begin ,end &aux (,tid (work-queue:worker-id)))
                      (declare (type fixnum ,begin ,end ,tid))
                      (flet ((work-queue:worker-id ()
                               ,tid))
                        (declare (inline work-queue:worker-id)
                                 (ignorable #'work-queue:worker-id))
                        (loop for ,i of-type fixnum from ,begin below ,end
                              do
                                 (let ((,var ,i))
                                   ,@body))))
                    ,(and result
                          `(lambda ()
                             (let ((,var nil))
                               (declare (ignorable ,var))
                               (progn ,result))))))))

(declaim (maybe-inline parallel:map parallel:reduce parallel:map-group-reduce))
(defun parallel:map (type function arg &key (wait t))
  (let* ((arg (coerce arg 'simple-vector))
         (function (if (functionp function)
                       function
                       (fdefinition function)))
         (future (if (eql nil type)
                     (parallel:dotimes (i (length arg))
                       (funcall function (aref arg i)))
                     (let ((destination (make-array (length arg))))
                       (parallel:dotimes (i (length arg) (coerce destination type))
                         (setf (aref destination i)
                               (funcall function (aref arg i))))))))
    (if wait
        (future-value future)
        future)))

(defun parallel:reduce (function arg seed &key (wait t) key)
  (let* ((arg (coerce arg 'simple-vector))
         (function (if (functionp function)
                       function
                       (fdefinition function)))
         (accumulators (make-array (work-queue:worker-count
                                    (work-queue:current-queue
                                     parallel-future:*context*))
                                   :initial-element seed))
         (future
           (if key
               (let ((key (if (functionp key) key (fdefinition key))))
                 (parallel:dotimes (i (length arg)
                                      (reduce function accumulators
                                              :initial-value seed))
                   (let ((idx (work-queue:worker-id)))
                     (setf (aref accumulators idx)
                           (funcall function
                                    (aref accumulators idx)
                                    (funcall key (aref arg i)))))))
               (parallel:dotimes (i (length arg)
                                    (reduce function accumulators
                                            :initial-value seed))
                 (let ((idx (work-queue:worker-id)))
                   (setf (aref accumulators idx)
                         (funcall function
                                  (aref accumulators idx)
                                  (aref arg i))))))))
    (if wait
        (future-value future)
        future)))

(defun parallel:map-group-reduce (sequence map reduce
                                  &key (group-test #'eql)
                                       group-by
                                       (wait t)
                                       (master-table nil)
                                       fancy)
  (let* ((arg     (coerce sequence 'simple-vector))
         (nthread (work-queue:worker-count
                   (work-queue:current-queue
                    parallel-future:*context*)))
         (tables  (map-into (make-array nthread)
                            (lambda () (make-hash-table :test group-test))))
         (map     (if (functionp map) map (fdefinition map)))
         (reduce  (if (functionp reduce) reduce (fdefinition reduce)))
         (group-by (and group-by
                        (if (functionp group-by) group-by (fdefinition group-by)))))
    (declare (type (simple-array hash-table 1) tables))
    (labels ((clean-table (table)
               (declare (type hash-table table))
               (ecase master-table
                 ((nil) nil)
                 (:quick table)
                 (t
                  (maphash (lambda (k v)
                             (setf (gethash k table) (cdr v)))
                           table)
                  table)))
             (aggregate-keys ()
               (let* ((size   (reduce #'max tables :key #'hash-table-count))
                      (master (make-hash-table :test group-test :size size))
                      (vector (make-array size :adjustable t :fill-pointer 0)))
                 (map nil (lambda (table)
                            (maphash (lambda (k v)
                                       (let ((cache (gethash k master)))
                                         (if cache
                                             (push v (cdr cache))
                                             (let ((cache (cons k (list v))))
                                               (vector-push-extend cache vector)
                                               (setf (gethash k master) cache)))))
                                     table))
                      tables)
                 (let ((vector (coerce (shiftf vector nil) 'simple-vector)))
                   (declare (type (simple-array cons 1) vector))
                   (parallel:dotimes (i (length vector)
                                        (values vector (clean-table master)))
                     (let* ((cache  (aref vector i))
                            (values (cdr cache)))
                       (setf (cdr cache) (reduce reduce values)))))))
             (accumulate (table key val)
               (declare (type hash-table table))
               (multiple-value-bind (acc foundp)
                   (gethash key table)
                 (setf (gethash key table)
                       (if foundp
                           (funcall reduce acc val)
                           val)))))
      (declare (inline accumulate))
      (let ((future (parallel:dotimes (i (length arg) (aggregate-keys))
                      (let* ((x     (aref arg i))
                             (table (aref tables (work-queue:worker-id))))
                        (declare (type hash-table table))
                        (if fancy
                            (funcall map x (lambda (key value)
                                             (accumulate table key value)))
                            (multiple-value-bind (val key)
                                (if group-by
                                    (values (funcall map x)
                                            (funcall group-by x))
                                    (funcall map x))
                              (accumulate table key val)))))))
        (if wait
            (future-value (future-value future))
            future)))))

#||

(defun count-words (documents)
  (parallel:map-group-reduce documents
                             (lambda (document accumulator)
                               (map nil (lambda (word)
                                          (funcall accumulator word 1))
                                    document))
                             #'+
                             :group-test #'equal
                             :fancy t))

(deftype index ()
  `(mod ,most-positive-fixnum))

;; todo: three-way partition?

(declaim (inline selection-sort partition find-pivot))
(defun partition (vec begin end pivot)
  (declare (type (simple-array fixnum 1) vec)
           (type index begin end)
           (type fixnum pivot)
           (optimize speed))
  (loop while (> end begin)
        do (if (<= (aref vec begin) pivot)
               (incf begin)
               (rotatef (aref vec begin)
                        (aref vec (decf end))))
        finally (return begin)))

(defun selection-sort (vec begin end)
  (declare (type (simple-array fixnum 1) vec)
           (type index begin end)
           (optimize speed))
  (loop for dst from begin below end
        do
           (let ((min   (aref vec dst))
                 (min-i dst))
             (declare (type fixnum min)
                      (type index min-i))
             (loop for i from (1+ dst) below end
                   do (let ((x (aref vec i)))
                        (when (< x min)
                          (setf min   x
                                min-i i))))
             (rotatef (aref vec dst) (aref vec min-i)))))

(defun find-pivot (vec begin end)
  (declare (type (simple-array fixnum 1) vec)
           (type index begin end)
           (optimize speed))
  (let ((first  (aref vec begin))
        (last   (aref vec (1- end)))
        (middle (aref vec (truncate (+ begin end) 2))))
    (declare (type fixnum first last middle))
    (when (> first last)
      (rotatef first last))
    (cond ((< middle first)
           first
           (setf middle first))
          ((> middle last)
           last)
          (t
           middle))))

(defun pqsort (vec)
  (declare (type (simple-array fixnum 1) vec)
           (optimize speed))
  (labels ((rec (begin end)
             (declare (type index begin end))
             (when (<= (- end begin) 8)
               (return-from rec (selection-sort vec begin end)))
             (let* ((pivot (find-pivot vec begin end))
                    (split (partition vec begin end pivot)))
               (declare (type fixnum pivot)
                        (type index  split))
               (cond ((= split begin)
                      (let ((next (position pivot vec
                                            :start    begin
                                            :end      end
                                            :test-not #'eql)))
                        (assert (> next begin))
                        (rec next end)))
                     ((= split end)
                      (let ((last (position pivot vec
                                            :start    begin
                                            :end      end
                                            :from-end t
                                            :test-not #'eql)))
                        (assert last)
                        (rec begin last)))
                     (t
                      (parallel:let ((left  (rec begin split))
                                     (right (rec split end))
                                     (:parallel (>= (- end begin) 512)))
                        (declare (ignore left right))))))))
    (rec 0 (length vec))
    vec))

(defun shuffle (vector)
  (declare (type vector vector))
  (let ((end (length vector)))
    (loop for i from (- end 1) downto 0
          do (rotatef (aref vector i) 
                      (aref vector (random (+ i 1)))))
    vector))

(defun test-pqsort (nproc size)
  (let ((vec (shuffle (let ((i 0))
                        (map-into (make-array size :element-type 'fixnum)
                                  (lambda ()
                                    (incf i)))))))
    (parallel-future:with-context (nproc)
      (time (pqsort vec)))
    (loop for i below (1- (length vec))
          do (assert (<= (aref vec i) (aref vec (1+ i)))))))

(defun test-sort (size)
  (let ((vec (shuffle (let ((i 0))
                        (map-into (make-array size :element-type 'fixnum)
                                  (lambda ()
                                    (incf i)))))))
    (declare (type (simple-array fixnum 1) vec))
    (time (locally (declare (optimize speed (space 0))
                            (inline sort))
            (sort vec #'<)))
    
    (loop for i below (1- (length vec))
            do (assert (<= (aref vec i) (aref vec (1+ i)))))))

;; SBCL sort (heap sort...)
* (test-sort (ash 1 25))

Evaluation took:
  15.870 seconds of real time
  15.828989 seconds of total run time (15.828989 user, 0.000000 system)
  99.74% CPU
  44,325,352,312 processor cycles
  0 bytes consed

;; without any parallelism machinery
* (test-pqsort 1 (ash 1 25))

Evaluation took:
  6.245 seconds of real time
  6.236389 seconds of total run time (6.236389 user, 0.000000 system)
  99.86% CPU
  17,440,707,947 processor cycles
  0 bytes consed

;; with parallelism
* (test-pqsort 1 (ash 1 25))
  
Evaluation took:
  6.420 seconds of real time
  6.416401 seconds of total run time (6.416401 user, 0.000000 system)
  99.94% CPU
  17,930,818,675 processor cycles
  45,655,456 bytes consed
  
NIL
* (test-pqsort 2 (ash 1 25))

Evaluation took:
  3.374 seconds of real time
  6.572410 seconds of total run time (6.572410 user, 0.000000 system)
  194.78% CPU
  9,422,768,541 processor cycles
  45,555,680 bytes consed
  
NIL
* (test-pqsort 4 (ash 1 25))

Evaluation took:
  1.794 seconds of real time
  6.536409 seconds of total run time (6.532409 user, 0.004000 system)
  364.33% CPU
  5,010,358,913 processor cycles
  45,502,272 bytes consed
  
NIL
* (test-pqsort 8 (ash 1 25))

Evaluation took:
  1.263 seconds of real time
  8.456529 seconds of total run time (8.452529 user, 0.004000 system)
  669.60% CPU
  3,525,995,357 processor cycles
  45,649,552 bytes consed
  
NIL
* (test-pqsort 11 (ash 1 25))

Evaluation took:
  1.153 seconds of real time
  9.188575 seconds of total run time (9.184574 user, 0.004001 system)
  796.96% CPU
  3,219,159,980 processor cycles
  45,678,192 bytes consed

NIL
||#
