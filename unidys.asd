(asdf:defsystem unidys
  :name "unidys"
  :version "1"
  :maintainer "codr7"
  :author "codr7"
  :description ""
  :licence "MIT"
  :build-operation "asdf:program-op"
  :build-pathname "unidys"
  :entry-point "unidys:main"
  :depends-on ("cffi" "local-time")
  :serial t
  :components ((:file "util")
	       (:file "pg")
	       (:file "db")
	       (:file "unidys")))
