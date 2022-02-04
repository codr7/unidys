(defpackage unidys
  (:use cl unidys-db)
  (:import-from local-time now timestamp)
  (:export))

(in-package unidys)

(defvar *db*)

(defstruct (user (:include struct-model))
  (name "" :type string)
  (created-at (error "missing created-at") :type timestamp)
  (last-login-at (error "missing last-login-at") :type timestamp)
  (last-logout-at (error "missing last-logout-at") :type timestamp))

(defun new-user (name)
  (let ((now (now)))
    (make-user :name name :created-at now :last-login-at now :last-logout-at now)))

(defmethod unidys-db:model-table ((self user))
  (users *db*))

(defstruct (folder (:include struct-model))
  (name "" :type string)
  (created-at (error "missing created-at") :type timestamp))

(defclass db ()
  ((connection :initarg :connection :reader connection)
   (users-name :reader users-name)
   (users-created-at :reader users-created-at)
   (users-last-login-at :reader users-last-login-at)
   (users-last-logout-at :reader users-last-logout-at)
   (users :initarg :users :reader users)
   (folders-name :reader folders-name)
   (folders-created-at :reader folders-created-at)
   (folders :initarg :folders :reader folders)))

(defun new-db (&key (connection *connection*))
  (let ((*db* (make-instance 'db :connection connection)))
    (with-slots (folders folders-name folders-created-at
		 users users-name users-created-at users-last-login-at users-last-logout-at) *db*
      (setf users-name (new-string-column 'name))
      (setf users-created-at (new-timestamp-column 'created-at))
      (setf users-last-login-at (new-timestamp-column 'last-login-at))
      (setf users-last-logout-at (new-timestamp-column 'last-logout-at))
      (setf users (new-table 'users '(name) (list users-name users-created-at users-last-login-at users-last-logout-at)))

      (setf folders-name (new-string-column 'name))
      (setf folders-created-at (new-timestamp-column 'created-at))
      (setf folders (new-table 'folders '(name) (list folders-name folders-created-at)))

      (table-drop users)
      (table-drop folders)
      
      (unless (table-exists? users)
	(table-create users)
	(let ((admin (new-user "admin")))
	  (model-store admin)))
      (unless (table-exists? folders)
	(table-create folders))
    *db*)))

(defmacro with-db ((&rest args) &body body)
  `(with-connection (,@args)
     (let ((*db* (new-db :connection *connection*)))
     ,@body)))

(defun tests ()
  (with-db ("test" "test" "test")))
    
