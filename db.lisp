(defpackage unidys-db
  (:use cffi cl unidys-pg)
  (:import-from unidys-util str! sym! syms!)
  (:import-from local-time format-timestring parse-timestring timestamp)
  (:export *connection*
	   boolean-column
	   class-model column
	   definition exists?
	   model-load model-store model-table
	   name new-boolean-column new-string-column new-timestamp-column new-table
	   string-column struct-model
	   table table-create table-drop table-exists? timestamp-column
	   with-connection))

(in-package unidys-db)

(defvar *connection*)
(defparameter *debug* t)

(defun connection-ok? (&key (connection *connection*))
  (eq (PQstatus connection) :CONNECTION_OK))

(defun connect (db user password &key (host "localhost"))
  (let ((c (PQconnectdb (format nil "postgresql://~a:~a@~a/~a" user password host db))))
    (unless (connection-ok? :connection c)
      (error (PQerrorMessage c)))
    c))

(defun send-query (sql params &key (connection *connection*))
  (when *debug*
    (format t "~a~%" sql)
    (when params
      (format t "~a~%" params)))

  (let ((nparams (length params)))
    (with-foreign-object (cparams :pointer nparams)
      (let ((i 0))
	(dolist (p params)
	  (setf (mem-aref cparams :pointer i) (foreign-string-alloc p))
	  (incf i)))
      
      (unless (= (PQsendQueryParams connection
				    sql
				    nparams
				    (null-pointer) cparams (null-pointer) (null-pointer)
				    0)
		 1)
	(error (PQerrorMessage connection)))
      
      (dotimes (i (length params))
	(foreign-string-free (mem-aref cparams :pointer i))))))

(defun get-result (&key (connection *connection*))
  (let* ((r (PQgetResult connection)))
    (if (null-pointer-p r)
	(values nil nil)
	(let ((s (PQresultStatus r)))
	  (unless (or (eq s :PGRES_COMMAND_OK) (eq s :PGRES_TUPLES_OK))
	    (error "~a~%~a" s (PQresultErrorMessage r)))
	  (values r s)))))

(defmacro with-connection ((&rest args) &body body)
  `(let ((*connection* (connect ,@args)))
     ,@body
     (PQfinish *connection*)))

(defmethod to-sql ((self string))
  (let ((out (copy-seq self)))
    (dotimes (i (length out))
      (let ((c (char out i)))
	(when (char= c #\-)
	  (setf (char out i) #\_))))
    out))

(defmethod to-sql ((self symbol))
  (to-sql (string-downcase (symbol-name self))))

(defclass definition ()
  ((name :initarg :name :initform (error "missing name") :reader name)))

(defmethod to-sql ((self definition))
  (to-sql (name self)))

(defclass column (definition)
  ())

(defmacro define-column-type (name data-type)
  `(progn
    (defclass ,name (column)
      ())
    
    (defun ,(syms! 'new- name) (name)
      (make-instance ',name :name name))
    
    (defmethod data-type ((self ,name))
      ,data-type)))

(define-column-type boolean-column "BOOLEAN")

(defun boolean-encode (val)
  (if val "t" "f"))

(defmethod column-encode ((self boolean-column) val)
  (boolean-encode val))

(defun boolean-decode (val)
  (string= val "t"))

(defmethod column-decode ((self boolean-column) val)
  (boolean-decode val))

(define-column-type string-column "TEXT")

(defmethod column-encode ((self string-column) val)
  val)

(defmethod column-decode ((self string-column) val)
  val)

(define-column-type timestamp-column "TIMESTAMP")

(defun timestamp-encode (val)
  (format-timestring nil val)) 

(defmethod column-encode ((self timestamp-column) val)
  (timestamp-encode val))

(defun timestamp-decode (val)
  (parse-timestring val))

(defmethod column-decode ((self timestamp-column) val)
  (timestamp-decode val))

(defclass relation ()
  ((columns :initform (make-array 0 :element-type 'column :fill-pointer 0) :reader columns)
   (column-lookup :initform (make-hash-table))))

(defclass key (definition relation)
  ())

(defun new-key (name cols)
  (let ((key (make-instance 'key :name name)))
    (with-slots (columns column-lookup) key
      (dolist (c cols)
	(setf (gethash (name c) column-lookup) (length columns))
	(vector-push-extend c columns)))
    key))

(defmacro do-columns ((col rel) &body body)
  (let ((i (gensym)))
    `(dotimes (,i (length (columns ,rel)))
       (let ((,col (aref (columns ,rel) ,i)))
	 ,@body))))

(defun key-add (self tbl)
  (let ((sql (with-output-to-string (out)
	       (format out "ALTER TABLE ~a ADD CONSTRAINT ~a ~a ("
		       (to-sql tbl)
		       (to-sql self)
		       (if (eq self (primary-key tbl)) "PRIMARY KEY" "UNIQUE"))
	       
	       (let ((i 0))
		 (do-columns (c self)
		   (unless (zerop i)
		     (format out ", "))
		   (format out "~a" (to-sql c))
		   (incf i)))
	       
	       (format out ")"))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defclass foreign-key (key)
  ((table :initarg :table :initform (error "missing table") :reader table)
   (key-columns :initarg :key-columns :initform (error "missing key-columns"))))

(defun new-foreign-key (name tbl cols)
  (let ((key (make-instance 'foreign-key :name name :table tbl)))
    (with-slots (columns column-lookup key-columns) key
      (dolist (fc cols)
	(let ((c (column-clone (syms! name '- (name fc)))))
	  (setf (gethash (name c) column-lookup) (length columns))
	  (vector-push-extend c columns))
	(push fc key-columns))
      (setf key-columns (nreverse key-columns)))
    key))

(defclass table (definition relation)
  ((primary-key :reader primary-key)))

(defun map-columns (body rel)
  (let ((cs (columns rel)) out)
    (dotimes (i (length cs))
      (push (funcall body (aref cs i)) out))
    (nreverse out)))

(defun new-table (name primary-cols cols)
  (let ((tbl (make-instance 'table :name name)))
    (with-slots (columns column-lookup primary-key) tbl
      (dolist (c cols)
	(setf (gethash (name c) column-lookup) (length columns))
	(vector-push-extend c columns))
      
      (setf primary-key
	    (new-key (intern (format nil "~a-primary" name))
		     (mapcar (lambda (c) (aref columns (gethash c column-lookup))) primary-cols))))
    tbl))

(defun table-exists? (self)
  (send-query "SELECT EXISTS (
                 SELECT FROM pg_tables
                 WHERE tablename  = $1
               )"
	      (list (str! (name self))))
  (let ((r (get-result)))
    (assert (= (PQntuples r) 1))
    (assert (= (PQnfields r) 1))
    (let ((result (boolean-decode (PQgetvalue r 0 0))))
      (PQclear r)
      (assert (null (get-result)))
      result)))

(defmethod exists? ((self table))
  (table-exists? self))

(defun table-create (self)
  (let ((sql (with-output-to-string (out)
	       (format out "CREATE TABLE ~a (" (to-sql self))
	       (with-slots (columns) self
		 (dotimes (i (length columns))
		   (let ((c (aref columns i)))
		     (unless (eq c (aref columns 0))
		       (format out ", "))
		     (format out "~a ~a" (to-sql c) (data-type c))))
		 (format out ")")))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  (assert (null (get-result)))
  (key-add (primary-key self) self))

(defmethod create ((self table))
  (table-create self))

(defun table-drop (self)
  (let ((sql (format nil "DROP TABLE ~a" (to-sql self))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defmethod drop ((self table))
  (table-drop self))

(defun load-record (tbl key-cond)
  (let ((sql (with-output-to-string (out)
	       (format out "SELECT ")
	       (let ((i 0))
		 (do-columns (c tbl)
		   (unless (zerop i)
		     (format out ", "))
		   (format out (to-sql c))
		   (incf i)))

	       (format out " FROM ~a WHERE " (to-sql tbl))
	       
	       (let ((param-count 0))
		 (dolist (k key-cond)
		   (format out "~a=$~a" (to-sql (first k)) (incf param-count)))))))
    (send-query sql (mapcar #'rest key-cond)))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_TUPLES_OK))
    (let (out (i 0))
      (do-columns (c tbl)
	(push (cons c (column-decode c (PQgetvalue r 0 i))) out)
	(incf i))
      (PQclear r)
      (assert (null (get-result)))
      out)))

(defun insert-record (tbl rec)
  (let ((sql (with-output-to-string (out)
	       (format out "INSERT INTO ~a (" (to-sql tbl))
	       (with-slots (column-lookup) tbl
		 (dolist (f rec)
		   (let ((c (first f)))
		     (unless (gethash (name c) column-lookup)
		       (error "unknown column: ~a" c))
		     (unless (eq f (first rec))
		       (format out ", "))
		     (format out "~a" (to-sql c)))))
	       (format out ") VALUES (")
	       (dotimes (i (length rec))
		 (unless (zerop i)
		   (format out ", "))
		 (format out "$~a" (1+ i)))
	       (format out ")"))))
    (send-query sql (mapcar #'rest rec)))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defun update-record (tbl rec)
  (let ((sql (with-output-to-string (out)
	       (format out "UPDATE ~a SET " (to-sql tbl))
	       (let ((param-count 0))
		 (with-slots (column-lookup columns) tbl
		   (dolist (f rec)
		     (let ((c (first f)))
		       (unless (gethash c column-lookup)
			 (error "unknown column: ~a" c))
		       (unless (eq f (first rec))
			 (format out ", "))
		       (format out "~a=$~a" (to-sql c) (incf param-count)))))
		 (format out " WHERE "))
	       (let ((i 0))
		 (dolist (f rec)
		   (unless (zerop i)
		     (format out " AND "))
		   (format out "~a=$~a" (to-sql (first f)) (incf i))))
	       (format out ")"))))
    (send-query sql (mapcar #'rest rec)))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defstruct reference
  (table (error "missing table") :type table)
  (key-cond (error "missing key") :type list)
  (record nil))

(defun new-reference (table key-cond)
  (make-reference :table table :key-cond key-cond))
  
(defun get-record (self)
  (or (reference-record self) (load-record (reference-table self) (reference-key-cond self))))

(defstruct struct-model
  (exists? nil :type boolean))

(defmethod exists? ((self struct-model))
  (slot-value self 'exists?))

(defclass class-model ()
  ((exists? :initarg :exists? :initform nil :reader exists?)))

(defmethod model-table (self))

(defmethod model-load (self rec)
    (let* ((tbl (model-table self)))
      (dolist (f rec)
	(let* ((c (first f))
	       (cn (name c)))
	  (with-slots (column-lookup) tbl
	    (when (gethash cn column-lookup)
	      (setf (slot-value self cn) (column-decode c (rest (assoc rec c)))))))))
    rec)

(defmethod model-store (self)
  (let* ((tbl (model-table self))
	 (rec (map-columns (lambda (c) (cons c (column-encode c (slot-value self (name c))))) tbl)))
    (with-slots (exists?) self
      (if exists?
	  (update-record tbl rec)
	  (progn
	    (insert-record tbl rec)
	    (setf exists? t))))))

(defun tests ()
  (with-connection ("test" "test" "test")
    (when (not (connection-ok?))
      (error (PQerrorMessage *connection*)))

    (send-query "SELECT * FROM pg_tables" '())
    
    (let* ((r (get-result)))
      (assert (eq (PQresultStatus r) :PGRES_TUPLES_OK))
      (PQclear r))
    (assert (null (get-result)))
    
    (let ((tbl (new-table 'foo '(bar) (list (new-string-column 'bar)))))
      (assert (not (exists? tbl)))
      (create tbl)
      (assert (exists? tbl))
      (drop tbl)
      (assert (not (exists? tbl))))))
