(defpackage unidys
  (:use cl unidys-db)
  (:import-from local-time now timestamp)
  (:import-from unidys-util syms!)
  (:export))

(in-package unidys)

(define-db unidys-db
  (table users (name)
	 (column name string)
	 (column created-at timestamp))
  (table rcs (name)
	 (column name string)
	 (column created-at string)
	 (foreign-key created-by users)))

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
  (created-by (new-rec-proxy (find-table 'users)) :type rec-proxy))

(defun new-rc (name)
  (make-rc :name name))

(defmethod unidys-db:model-table ((self rc))
  (find-table 'rcs))

(defmacro with-db ((&rest args) &body body)
  `(with-cx (,@args)
     (let* ((*db* (make-instance 'unidys-db)))
       ,@body)))

(defun db-init ()
  (drop-tables)
  
  (unless (table-exists? (find-table 'users))
    (create-tables)
    
    (let* ((admin (new-user "admin")))
      (model-store admin))))

(defun main ()
  (with-db ("test" "test" "test")
    (db-init)))

(defun tests ()
  (with-db ("test" "test" "test")
    (db-init)))

