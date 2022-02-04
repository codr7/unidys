(defpackage unidys
  (:use cl unidys-db)
  (:import-from local-time *default-timezone* +utc-zone+ encode-timestamp now timestamp)
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
  (table rcs (name)
	 (column name string)
	 (column created-at timestamp)
	 (foreign-key created-by users))
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
  (find-table 'users))

(defstruct (rc (:include model))
  (name "" :type string)
  (created-at (now) :type timestamp)
  (created-by (new-model-proxy (find-table 'users) 'user) :type model-proxy))
		
(defun new-rc (name)
  (let ((self (make-rc :name name)))
    (set-model (rc-created-by self) *user*)
    self))

(defmethod unidys-db:model-table ((self rc))
  (find-table 'rcs))

(defstruct (cap (:include model))
  (rc (new-model-proxy (find-table 'rcs) 'rc) :type model-proxy)
  (starts-at (error "missing starts-at") :type timestamp)
  (ends-at (error "missing ends-at") :type timestamp)
  (total (error "missing total") :type integer)
  (used (error "missing used") :type integer))

(defun new-cap (rc starts-at ends-at total used)
  (let ((self (make-cap :starts-at starts-at :ends-at ends-at :total total :used used)))
    (set-model (cap-rc self) rc)
    self))

(defmethod unidys-db:model-store :after ((self rc))
  (model-store (new-cap self *min-timestamp* *max-timestamp* 0 0)))

(defmethod unidys-db:model-table ((self cap))
  (find-table 'caps))

(defmacro with-db ((&rest args) &body body)
  `(with-cx (,@args)
     (let* ((*db* (make-instance 'unidys-db)))
       ,@body)))

(defun db-init ()
  (drop-tables)
  
  (unless (table-exists? (find-table 'users))
    (create-tables)
    
    (let* ((*user* (new-user "admin")))
      (model-store *user*)
      
      (let* ((lodging (new-rc "lodging"))
	     (cabins (new-rc "cabins"))
	     (rooms (new-rc "rooms")))
	(model-store lodging)
	(model-store cabins)
	(model-store rooms)))))

(defun main ()
  (with-db ("test" "test" "test")
    (db-init)))

(defun tests ()
  (with-db ("test" "test" "test")
    (db-init)))

