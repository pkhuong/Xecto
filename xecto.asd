(asdf:defsystem "xecto"
  :version "0.0.0"
  :licence "BSD"
  :description "Xecto is a simple parallel vector-processing library"
  :components
  ((:file "mpsc-queue")
   (:file "thread-pool" :depends-on ("mpsc-queue"))
   (:file "futures")
   (:file "parallel-futures" :depends-on ("thread-pool" "futures"))
   (:file "vector-futures" :depends-on ("parallel-futures"))
   (:file "loop-nest-transpose")
   (:file "xecto-impl" :depends-on ("vector-futures" "loop-nest-transpose"))
   (:file "xecto-impl-reshape" :depends-on ("xecto-impl"))
   (:file "xecto-impl-map" :depends-on ("xecto-impl"))
   (:file "xecto-impl-reduce" :depends-on ("xecto-impl"))))
