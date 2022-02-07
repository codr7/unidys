(defpackage unidys
  (:use cl unidys-db unidys-pg)
  (:import-from local-time encode-timestamp now timestamp timestamp< timestamp>)
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
  (table rc-trees (parent child)
	 (foreign-key parent rcs)
	 (foreign-key child rcs))
  (table caps (rc starts-at)
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

(defun get-caps (rc &key (starts-at (now)) (ends-at *max-timestamp*))
  (let* ((sql (with-output-to-string (out)
		(format out "SELECT ")

		(let* ((i 0))
		  (do-columns (c (find-def 'caps))
		    (unless (zerop i)
		      (format out ", "))
		    (format out (to-sql c))
		    (incf i)))

		(format out " FROM caps where rc_name = $1 AND starts_at < $2 AND ends_at > $3"))))
    
    (send-query sql (list (rc-name rc) (to-sql ends-at) (to-sql starts-at))))

  (labels ((get-next (out)
	     (let* ((r (get-result)))
	       (if (and r (not (zerop (PQntuples r))))
		   (progn
		     (let* ((cap (new-cap rc)))
		       (model-load cap (load-rec (find-def 'caps) r))
		       (PQclear r)
		       (get-next (cons cap out))))
		   (nreverse out)))))
    (get-next nil)))

(defun update-caps (in &key (starts-at (now)) (ends-at *max-timestamp*) (total 0) (used 0))
  (let (out)
    (dolist (c in)
      (when (timestamp< (cap-starts-at c) starts-at)
	(let* ((prefix c))
	  (setf c (model-clone c 'starts-at starts-at))
	  (setf (cap-ends-at prefix) starts-at)
	  (push prefix out))
	
	(incf (cap-total c) total)
	(incf (cap-used c) used)
	(push c out)

	(when (timestamp> (cap-ends-at c) ends-at)
	  (let ((suffix (copy-structure c)))
	    (setf (cap-starts-at suffix) ends-at)
	    (setf (cap-ends-at c) ends-at)
	    (push suffix out)))))
    out))

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
	     (rooms (new-rc "rooms"))
	     (room1 (new-rc "room1" :type :fixed)))
	(model-store lodging)
	(model-store cabins)
	(rc-add-child lodging cabins)
	(model-store rooms)
	(rc-add-child lodging rooms)
	(model-store room1)
	(rc-add-child rooms room1)

	(let* ((cs1 (get-caps room1))
	       (cs2 (update-caps cs1 :total 1)))
	  (dolist (c cs2)
	    (model-store c)))))))

(defun main ()
  (with-db ("test" "test" "test")
    (db-init)))

(defun tests ()
  (with-db ("test" "test" "test")
    (db-init)))
