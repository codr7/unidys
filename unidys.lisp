(defpackage unidys
  (:use cl unidys-db)
  (:import-from local-time encode-timestamp now timestamp)
  (:import-from unidys-util syms!)
  (:export main tests))

(in-package unidys)

(defparameter *min-timestamp* (encode-timestamp 0 0 0 0 1 1 1))
(defparameter *max-timestamp* (encode-timestamp 0 0 59 23 31 12 9999))
  
(defvar *user*)

(define-db unidys-db
  (table users (name)
	 (column name string)
	 (column created-at timestamp))
  (enum rc-type
	automatic
	fixed
	unlimited)
  (table rcs (name)
	 (column name string)
	 (column type rc-type)
	 (column created-at timestamp)
	 (foreign-key created-by users))
  (table rc-trees (parent-name child-name)
	 (foreign-key parent rcs)
	 (foreign-key child rcs))
  (table caps (rc-name starts-at)
	 (foreign-key rc rcs)
	 (column starts-at timestamp)
	 (column ends-at timestamp)
	 (column total integer)
	 (column used integer)))

(defstruct (user (:include model))
  (name "" :type string)
  (created-at (now) :type timestamp))

(defun new-user (name)
  (make-user :name name))

(defmethod unidys-db:model-table ((self user))
  (find-def 'users))

(defstruct (cap (:include model))
  (rc (new-model-proxy (find-def 'rcs) 'rc) :type model-proxy)
  (starts-at *min-timestamp* :type timestamp)
  (ends-at *max-timestamp* :type timestamp)
  (total 0 :type integer)
  (used 0 :type integer))

(defun new-cap (rc &rest args)
  (let ((self (apply #'make-cap args)))
    (set-model (cap-rc self) rc)
    self))

(defmethod unidys-db:model-table ((self cap))
  (find-def 'caps))

(defstruct (rc (:include model))
  (name "" :type string)
  (type :automatic :type keyword)
  (created-at (now) :type timestamp)
  (created-by (new-model-proxy (find-def 'users) 'user) :type model-proxy))
		
(defun new-rc (name &rest args)
  (let ((self (apply #'make-rc :name name args)))
    (set-model (rc-created-by self) *user*)
    self))

(defmethod unidys-db:model-table ((self rc))
  (find-def 'rcs))

(defmethod unidys-db:model-store :after ((self rc))
  (model-store (new-cap self)))

(defstruct (rc-tree (:include model))
  (parent (new-model-proxy (find-def 'rcs) 'rc) :type model-proxy)
  (child (new-model-proxy (find-def 'rcs) 'rc) :type model-proxy))

(defun new-rc-tree (parent child &rest args)
  (let ((self (apply #'make-rc-tree args)))
    (set-model (rc-tree-parent self) parent)
    (set-model (rc-tree-child self) child)
    self))

(defmethod unidys-db:model-table ((self rc-tree))
  (find-def 'rc-trees))

(defun rc-add-child (parent child)
  (model-store (new-rc-tree parent child)))

(defmacro with-db ((&rest args) &body body)
  `(with-cx (,@args)
     (let* ((*db* (make-instance 'unidys-db)))
       ,@body)))

(defun db-init ()
  (drop *db*)
  
  (unless (exists? (find-def 'users))
    (create *db*)
    
    (let* ((*user* (new-user "admin")))
      (model-store *user*)
      
      (let* ((lodging (new-rc "lodging"))
	     (cabins (new-rc "cabins"))
	     (rooms (new-rc "rooms")))
	(model-store lodging)
	(model-store cabins)
	(rc-add-child lodging cabins)
	(model-store rooms)
	(rc-add-child lodging rooms)))))

(defun main ()
  (with-db ("test" "test" "test")
    (db-init)))

(defun tests ()
  (with-db ("test" "test" "test")
    (db-init)))
