(defpackage unidys-db
  (:use cffi cl unidys-pg)
  (:import-from unidys-util dohash kw! str! sym! syms!)
  (:import-from local-time encode-timestamp format-timestring timestamp)
  (:export *cx* *db*
	   boolean-column
	   column column-from-sql column-to-sql create
	   db define-db definition do-columns drop
	   exists?
	   find-def find-rec
	   get-key get-model get-rec get-result
	   integer-column
	   load-rec
	   model model-clone model-load model-proxy model-store model-table
	   name new-boolean-column new-foreign-key new-integer-column new-key new-model-proxy new-rec-proxy
	   new-string-column new-timestamp-column new-table
	   rec-proxy
	   set-key set-model set-rec send-query string-column
	   table table-create table-drop table-exists? timestamp-column to-sql
	   with-cx))

(in-package unidys-db)

(defvar *cx*)
(defvar *db*)

(defparameter *debug* t)

(defun cx-ok? (&key (cx *cx*))
  (eq (PQstatus cx) :CONNECTION_OK))

(defun connect (db user password &key (host "localhost"))
  (let* ((c (PQconnectdb (format nil "postgresql://~a:~a@~a/~a" user password host db))))
    (unless (cx-ok? :cx c)
      (error (PQerrorMessage c)))
    c))

(defun send-query (sql params &key (cx *cx*))
  (when *debug*
    (format t "~a~%" sql)
    (when params
      (format t "~a~%" params)))

  (let* ((nparams (length params)))
    (with-foreign-object (cparams :pointer nparams)
      (let* ((i 0))
	(dolist (p params)
	  (setf (mem-aref cparams :pointer i) (foreign-string-alloc p))
	  (incf i)))
      
      (unless (= (PQsendQueryParams cx
				    sql
				    nparams
				    (null-pointer) cparams (null-pointer) (null-pointer)
				    0)
		 1)
	(error (PQerrorMessage cx)))
      
      (dotimes (i (length params))
	(foreign-string-free (mem-aref cparams :pointer i))))))

(defun get-result (&key (cx *cx*))
  (let* ((r (PQgetResult cx)))
    (if (null-pointer-p r)
	(values nil nil)
	(let* ((s (PQresultStatus r)))
	  (unless (or (eq s :PGRES_COMMAND_OK) (eq s :PGRES_TUPLES_OK))
	    (error "~a~%~a" s (PQresultErrorMessage r)))
	  (values r s)))))

(defmacro with-cx ((&rest args) &body body)
  `(let* ((*cx* (connect ,@args)))
     ,@body
     (PQfinish *cx*)))

(defmethod to-sql ((self string))
  (let* ((out (copy-seq self)))
    (dotimes (i (length out))
      (let* ((c (char out i)))
	(when (char= c #\-)
	  (setf (char out i) #\_))))
    out))

(defmethod to-sql ((self symbol))
  (to-sql (string-downcase (symbol-name self))))

(defclass definition ()
  ((name :initarg :name :initform (error "missing name") :reader name)))

(defmethod to-sql ((self definition))
  (to-sql (name self)))

(defclass enum (definition)
  ((alts :initarg :alts :reader alts)))

(defun new-enum (name &rest alts)
  (make-instance 'enum :name name :alts (make-array (length alts) :element-type 'keyword :initial-contents alts)))

(defun enum-create (self)
  (let* ((sql (with-output-to-string (out)
		(format out "CREATE TYPE ~a AS ENUM (" (to-sql self))
		(with-slots (alts) self
		  (dotimes (i (length alts))
		    (unless (zerop i)
		      (format out ", "))
		    (format out "'~a'" (to-sql (aref alts i))))
		  (format out ")")))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  (assert (null (get-result))))

(defmethod create ((self enum))
  (enum-create self))

(defun enum-drop (self)
  (let* ((sql (format nil "DROP TYPE IF EXISTS ~a" (to-sql self))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defmethod drop ((self enum))
  (enum-drop self))

(defun enum-exists? (self)
  (send-query "SELECT EXISTS (
                 SELECT FROM pg_type
                 WHERE typname  = $1
               )"
	      (list (to-sql (name self))))
  (let* ((r (get-result)))
    (assert (= (PQntuples r) 1))
    (assert (= (PQnfields r) 1))
    (let* ((result (boolean-from-sql (PQgetvalue r 0 0))))
      (PQclear r)
      (assert (null (get-result)))
      result)))

(defmethod exists? ((self enum))
  (enum-exists? self))

(defclass column (definition)
  ())

(defmethod column-clone ((self column) name)
  (make-instance (type-of self) :name name))

(defmethod column-to-sql (self val)
  (to-sql val))

(defmacro define-column-type (name data-type)
  `(progn
     (defclass ,name (column)
       ())
     
     (defun ,(syms! 'new- name) (name)
       (make-instance ',name :name name))
     
     (defmethod data-type ((self ,name))
       ,data-type)))

(define-column-type boolean-column "BOOLEAN")

(defmethod boolean-to-sql (val)
  (if val "t" "f"))

(defmethod column-to-sql ((self boolean-column) val)
  (boolean-to-sql val))

(defun boolean-from-sql (val)
  (string= val "t"))

(defmethod column-from-sql ((self boolean-column) val)
  (boolean-from-sql val))

(define-column-type integer-column "INTEGER")

(defmethod to-sql ((self integer))
  (format nil "~a" self))

(defun integer-from-sql (val)
  (parse-integer val))

(defmethod column-from-sql ((self integer-column) val)
  (integer-from-sql val))

(define-column-type string-column "TEXT")

(defmethod column-to-sql ((self string-column) val)
  val)

(defmethod column-from-sql ((self string-column) val)
  val)

(define-column-type timestamp-column "TIMESTAMP")

(defmethod to-sql ((self timestamp))
  (format-timestring nil self)) 

(defun timestamp-from-sql (val)
  (flet ((p (i) (parse-integer val :start i :junk-allowed t)))
    (let* ((year (p 0))
	   (month (p 5))
	   (day (p 8))
	   (h (p 11))
	   (m (p 14))
	   (s (p 17)))
      (encode-timestamp 0 s m h day month year))))

(defmethod column-from-sql ((self timestamp-column) val)
  (timestamp-from-sql val))

(defclass relation ()
  ((columns :initform (make-array 0 :element-type 'column :fill-pointer 0) :reader columns)
   (column-indices :initform (make-hash-table))))

(defmacro do-columns ((col rel) &body body)
  (let* ((i (gensym)))
    `(dotimes (,i (length (columns ,rel)))
       (let* ((,col (aref (columns ,rel) ,i)))
	 ,@body))))

(defclass key (definition relation)
  ())

(defun new-key (name cols)
  (let* ((key (make-instance 'key :name name)))
    (with-slots (columns column-indices) key
      (dolist (c cols)
	(setf (gethash (name c) column-indices) (length columns))
	(vector-push-extend c columns)))
    key))

(defmethod key-create ((self key) table)
  (let* ((sql (with-output-to-string (out)
		(format out "ALTER TABLE ~a ADD CONSTRAINT ~a ~a ("
			(to-sql table)
			(to-sql self)
			(if (eq self (primary-key table)) "PRIMARY KEY" "UNIQUE"))
		
		(let* ((i 0))
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

(defmethod key-drop ((self key) table)
  (let* ((sql (format nil "ALTER TABLE ~a DROP CONSTRAINT IF EXISTS ~a"
		      (to-sql table) (to-sql self))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defclass foreign-key (key)
  ((table :initarg :table :initform (error "missing table") :reader table)
   (column-map :initform (make-hash-table) :reader column-map)))

(defun new-foreign-key (name table)
  (let* ((key (make-instance 'foreign-key :name name :table table)))
    (with-slots (column-indices column-map columns) key
      (do-columns (fc (primary-key table))
	(let* ((c (column-clone fc (syms! name '- (name fc)))))
	  (setf (gethash (name c) column-indices) (length columns))
	  (vector-push-extend c columns)
	  (setf (gethash c column-map) fc))))
    key))

(defmethod key-create ((self foreign-key) table)
  (let* ((sql (with-output-to-string (out)
		(format out "ALTER TABLE ~a ADD CONSTRAINT ~a FOREIGN KEY ("
			(to-sql table) (to-sql self))
		
		(let* ((i 0))
		  (dohash (c fc (column-map self))
		    (unless (zerop i)
		      (format out ", "))
		    (format out "~a" (to-sql c))
		    (incf i)))
		
		(format out ") REFERENCES ~a (" (to-sql (table self)))

		(let* ((i 0))
		  (dohash (c fc (column-map self))
		    (unless (zerop i)
		      (format out ", "))
		    (format out "~a" (to-sql fc))
		    (incf i)))
		
		(format out ")"))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defclass table (definition relation)
  ((primary-key :reader primary-key)
   (foreign-keys :initform nil :reader foreign-keys)
   (def-lookup :initform (make-hash-table))))

(defmethod print-object ((self table) out)
  (format out "(Table ~a)" (str! (name self))))

(defun map-columns (body rel)
  (let* ((cs (columns rel)) out)
    (dotimes (i (length cs))
      (push (funcall body (aref cs i)) out))
    (nreverse out)))

(defmethod table-add (table (col column))
  (with-slots (columns column-indices def-lookup) table
    (setf (gethash (name col) def-lookup) col)
    (setf (gethash (name col) column-indices) (length columns))
    (vector-push-extend col columns)))

(defmethod table-add (table (key foreign-key))
  (with-slots (def-lookup foreign-keys) table
    (setf (gethash (name key) def-lookup) key)
    (push key foreign-keys)
    
    (do-columns (c key)
      (table-add table c))))

(defun new-table (name primary-defs defs)
  (let* ((table (make-instance 'table :name name)))
    (dolist (d defs)
      (table-add table d))
    
    (with-slots (columns column-indices def-lookup primary-key) table
      (let* (primary-cols)
	(dolist (dk primary-defs)
	  (let ((d (gethash dk def-lookup)))
	    (etypecase d
	      (column (push d primary-cols))
	      (key (do-columns (c d) (push c primary-cols))))))
	(setf primary-key
	      (new-key (intern (format nil "~a-primary" name)) primary-cols))))
    
    table))

(defun table-exists? (self)
  (send-query "SELECT EXISTS (
                 SELECT FROM pg_tables
                 WHERE tablename  = $1
               )"
	      (list (to-sql (name self))))
  (let* ((r (get-result)))
    (assert (= (PQntuples r) 1))
    (assert (= (PQnfields r) 1))
    (let* ((result (boolean-from-sql (PQgetvalue r 0 0))))
      (PQclear r)
      (assert (null (get-result)))
      result)))

(defmethod exists? ((self table))
  (table-exists? self))

(defun table-create (self)
  (let* ((sql (with-output-to-string (out)
		(format out "CREATE TABLE ~a (" (to-sql self))
		(with-slots (columns) self
		  (dotimes (i (length columns))
		    (unless (zerop i)
		      (format out ", "))
		    
		    (let* ((c (aref columns i)))
		      (format out "~a ~a NOT NULL" (to-sql c) (data-type c))))
		  (format out ")")))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  
  (assert (null (get-result)))
  (key-create (primary-key self) self)
  
  (dolist (fk (foreign-keys self))
    (key-create fk self)))

(defmethod create ((self table))
  (table-create self))

(defun table-drop (self)
  (dolist (fk (foreign-keys self))
    (key-drop fk self))
  
  (let* ((sql (format nil "DROP TABLE IF EXISTS ~a" (to-sql self))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defmethod drop ((self table))
  (table-drop self))

(defun load-rec (table result &key rec (offset 0))
  (let* ((i offset))
    (do-columns (c table)
      (push (cons c (PQgetvalue result 0 i)) rec)
      (incf i))
    rec))

(defun find-rec (table key)
  (let* ((sql (with-output-to-string (out)
		(format out "SELECT ")
		(let* ((i 0))
		  (do-columns (c table)
		    (unless (zerop i)
		      (format out ", "))
		    (format out (to-sql c))
		    (incf i)))

		(format out " FROM ~a WHERE " (to-sql table))
		
		(let* ((param-count 0))
		  (dolist (k key)
		    (format out "~a=$~a" (to-sql (first k)) (incf param-count)))))))
    (send-query sql (mapcar #'rest key)))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_TUPLES_OK))
    (let ((rec (load-rec table r)))
      (PQclear r)
      (assert (null (get-result)))
      rec)))

(defun insert-rec (table rec)
  (let* ((sql (with-output-to-string (out)
		(format out "INSERT INTO ~a (" (to-sql table))
		(with-slots (column-indices) table
		  (dolist (f rec)
		    (let* ((c (first f)))
		      (unless (gethash (name c) column-indices)
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

(defun update-rec (table rec)
  (let* ((sql (with-output-to-string (out)
		(format out "UPDATE ~a SET " (to-sql table))
		(let* ((param-count 0))
		  (with-slots (column-indices columns) table
		    (dolist (f rec)
		      (let* ((c (first f)))
			(unless (gethash (name c) column-indices)
			  (error "unknown column: ~a" c))
			(unless (eq f (first rec))
			  (format out ", "))
			(format out "~a=$~a" (to-sql c) (incf param-count)))))
		  (format out " WHERE "))
		(let* ((i 0))
		  (dolist (f rec)
		    (unless (zerop i)
		      (format out " AND "))
		    (format out "~a=$~a" (to-sql (first f)) (incf i)))))))
    (send-query sql (mapcar #'rest rec)))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defstruct rec-proxy
  (table (error "missing table") :type table)
  (key-values (error "missing key-values") :type vector))

(defun new-rec-proxy (table)
  (make-rec-proxy :table table :key-values (make-array (length (columns (primary-key table))))))

(defmethod get-key ((self rec-proxy) i)
  (with-slots (key-values) self
    (aref key-values i)))

(defmethod set-key ((self rec-proxy) i val)
  (with-slots (key-values) self
    (setf (aref key-values i) val)))

(defun get-rec (self)
  (with-slots (key-values) self
    (with-slots (column-indices columns) (table self)
      (let* (k)
	(dotimes (i (length columns))
	  (push (cons (aref columns i) (aref key-values i)) k))
	(find-rec (table self) k)))))

(defun set-rec (self rec)
  (with-slots (key-values) self
    (let ((i 0))
      (do-columns (c (primary-key (rec-proxy-table self)))
	(setf (aref key-values i) (rest (assoc c rec)))))))

(defstruct (model-proxy (:include rec-proxy))
  (type-name (error "missing type-name") :type symbol))

(defun new-model-proxy (table type-name)
  (make-model-proxy :table table
		    :key-values (make-array (length (columns (primary-key table))))
		    :type-name type-name))

(defun set-model (self rec)
  (with-slots (key-values) self
    (let ((i 0))
      (do-columns (c (primary-key (rec-proxy-table self)))
	(setf (aref key-values i) (slot-value rec (name c)))))))

(defun get-model (self)
  (let* ((rec (get-rec self))
	 args)
    (dolist (f rec)
      (push (rest f) args)
      (push (kw! (name (first f))) args))
    (apply (syms! 'make- (model-proxy-type-name self)) args)))
    
(defstruct model
  (exists? nil :type boolean))

(defmethod exists? ((self model))
  (slot-value self 'exists?))

(defmethod model-table (self)
  (error "not implemented"))

(defun model-clone (in &rest updates)
  (let ((out (copy-structure in)))
    (setf (slot-value out 'exists?) nil)
    
    (labels ((next ()
	       (let* ((k (pop updates))
		      (v (pop updates)))
		 (when k
		   (setf (slot-value out k) v)
		   (next)))))
      (next))
    out))		

(defmethod model-load (self rec)
  (let* ((table (model-table self)))
    (dolist (f rec)
      (let* ((c (first f))
	     (k (name c)))
	(with-slots (column-indices) table
	  (when (slot-exists-p self k)
	    (when (gethash k column-indices)
	      (setf (slot-value self k) (column-from-sql c (rest f))))))))
    
      (with-slots (column-indices columns) table
	(dolist (fk (foreign-keys table))
	  (let* ((rp (slot-value self (name fk)))
		 (cs (columns fk)))
	    (dotimes (i (length cs))
	      (let* ((c (aref cs i)))
		(set-key rp i (column-from-sql c (rest (assoc c rec))))))))))
  
  (setf (slot-value self 'exists?) t)
  self)

(defmethod model-store (self)
  (let* ((table (model-table self))
	 rec) 
    (do-columns (c table)
      (when (slot-exists-p self (name c))
	(push (cons c (column-to-sql c (slot-value self (name c)))) rec)))
    
    (dolist (fk (foreign-keys table))
      (let* ((rp (slot-value self (name fk)))
	     (cs (columns fk)))
	(dotimes (i (length cs))
	  (let* ((c (aref cs i)))
	    (push (cons c (column-to-sql c (get-key rp i))) rec)))))
    
    (with-slots (exists?) self
      (if exists?
	  (update-rec table rec)
	  (progn
	    (insert-rec table rec)
	    (setf exists? t))))))

(defclass db ()
  ((defs :initform nil :reader defs)
   (def-lookup :initform (make-hash-table) :reader def-lookup)))

(defun find-def (name &key (db *db*))
  (gethash name (def-lookup db)))

(defmacro define-db (name &body forms)
  (let* (init-forms)
    (labels ((parse-table (f)
	       (let* ((name (pop f))
		      (key-cols (pop f))
		      defs)
		 (labels ((parse-column (f)
			    (let* ((name (pop f))
				   (type (pop f)))
			      (push `(,(syms! 'new- type '-column) ',name ,@f) defs)))
			  (parse-foreign-key (f)
			    (let* ((name (pop f))
				   (table (pop f)))
			      (push `(new-foreign-key ',name (find-def ',table :db self) ,@f) defs)))
			  (parse-table-form (f)
			    (ecase (kw! (first f))
			      (:column
			       (parse-column (rest f)))
			      (:foreign-key
			       (parse-foreign-key (rest f))))))
		   (dolist (tf f)
		     (parse-table-form tf)))
		 
		 (push `(let* ((table (new-table ',name '(,@key-cols) (list ,@(nreverse defs)))))
			  (push table defs)
			  (setf (gethash ',name def-lookup) table))
		       init-forms)))
	     (parse-enum (f)
	       (let* ((name (first f))
		      (ct (syms! name '-column)))
		 (push `(let* ((enum (new-enum ',name ,@(mapcar #'kw! (rest f)))))
			  (push enum defs)
			  (setf (gethash ',name def-lookup) enum)
			  (define-column-type ,ct ,(to-sql name))
			  
			  (defmethod column-to-sql ((self ,ct) val)
			    (str! val))
			  
			  (defmethod column-from-sql ((self ,ct) val)
			    (kw! val)))
		       init-forms)))
	     (parse-form (f)
	       (ecase (kw! (first f))
		 (:enum (parse-enum (rest f)))
		 (:table (parse-table (rest f))))))
      (dolist (f forms)
	(parse-form f))
      `(progn
	 (defclass ,name (db)
	   ())
	 
	 (defmethod initialize-instance :after ((self ,name) &key)
	   (with-slots (def-lookup defs) self
	     ,@(nreverse init-forms)))))))

(defmethod create ((self db))
  (dolist (d (reverse (defs self)))
    (create d)))

(defmethod drop ((self db))
  (dolist (d (defs self))
    (if (exists? d)
	(drop d))))

(defun tests ()
  (with-cx ("test" "test" "test")
    (when (not (cx-ok?))
      (error (PQerrorMessage *cx*)))

    (send-query "SELECT * FROM pg_tables" '())
    
    (let* ((r (get-result)))
      (assert (eq (PQresultStatus r) :PGRES_TUPLES_OK))
      (PQclear r))
    (assert (null (get-result)))
    
    (let* ((table (new-table 'foo '(bar) (list (new-string-column 'bar)))))
      (assert (not (exists? table)))
      (create table)
      (assert (exists? table))
      (drop table)
      (assert (not (exists? table))))))
