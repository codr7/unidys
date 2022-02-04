(defpackage unidys-db
  (:use cffi cl unidys-pg)
  (:import-from unidys-util dohash kw! str! sym! syms!)
  (:import-from local-time format-timestring parse-timestring timestamp)
  (:export *cx* *db*
	   boolean-column
	   column create-tables
	   db define-db definition drop-tables
	   exists?
	   find-table
	   get-key get-record
	   model model-load model-store model-table
	   name new-boolean-column new-foreign-key new-key new-rec-proxy new-string-column new-timestamp-column
	   new-table
	   rec-proxy
	   set-key string-column
	   table table-create table-drop table-exists? timestamp-column
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

(defclass column (definition)
  ())

(defmethod column-clone ((self column) name)
  (make-instance (type-of self) :name name))

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
   (foreign-keys :initform nil :reader foreign-keys)))

(defmethod print-object ((self table) out)
  (format out "(Table ~a)" (str! (name self))))

(defun map-columns (body rel)
  (let* ((cs (columns rel)) out)
    (dotimes (i (length cs))
      (push (funcall body (aref cs i)) out))
    (nreverse out)))

(defmethod table-add (table (col column))
  (with-slots (columns column-indices) table
    (setf (gethash (name col) column-indices) (length columns))
    (vector-push-extend col columns)))

(defmethod table-add (table (key foreign-key))
  (with-slots (foreign-keys) table
    (push key foreign-keys)
    
    (do-columns (c key)
      (table-add table c))))

(defun new-table (name primary-cols defs)
  (let* ((table (make-instance 'table :name name)))
    (dolist (d defs)
      (table-add table d))
    
    (with-slots (columns column-indices primary-key) table
      (setf primary-key
	    (new-key (intern (format nil "~a-primary" name))
		     (mapcar (lambda (c) (aref columns (gethash c column-indices))) primary-cols))))
    table))

(defun table-exists? (self)
  (send-query "SELECT EXISTS (
                 SELECT FROM pg_tables
                 WHERE tablename  = $1
               )"
	      (list (str! (name self))))
  (let* ((r (get-result)))
    (assert (= (PQntuples r) 1))
    (assert (= (PQnfields r) 1))
    (let* ((result (boolean-decode (PQgetvalue r 0 0))))
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
		    (let* ((c (aref columns i)))
		      (unless (eq c (aref columns 0))
			(format out ", "))
		      (format out "~a ~a" (to-sql c) (data-type c))))
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

(defun load-rec (table key)
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
    (let* (out (i 0))
      (do-columns (c table)
	(push (cons c (column-decode c (PQgetvalue r 0 i))) out)
	(incf i))
      (PQclear r)
      (assert (null (get-result)))
      out)))

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
			(unless (gethash c column-indices)
			  (error "unknown column: ~a" c))
			(unless (eq f (first rec))
			  (format out ", "))
			(format out "~a=$~a" (to-sql c) (incf param-count)))))
		  (format out " WHERE "))
		(let* ((i 0))
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

(defstruct rec-proxy
  (table (error "missing table") :type table)
  (key-values (error "missing key-values") :type vector)
  (cache nil))

(defun new-rec-proxy (table)
  (make-rec-proxy :table table :key-values (make-array (length (columns (primary-key table))))))

(defmethod get-key ((self rec-proxy) i)
  (with-slots (key-values) self
    (aref key-values i)))

(defmethod set-key ((self rec-proxy) i val)
  (with-slots (key-values) self
    (setf (aref key-values i) val)))

(defun get-rec (self)
  (or (rec-proxy-cache self)
      (with-slots (key-values) self
	(with-slots (column-indices columns) (table self)
	  (let* (k)
	    (dotimes (i (length columns))
	      (push (cons (aref columns i) (aref key-values i)) k))
	    (load-rec (table self) k))))))

(defstruct model
  (exists? nil :type boolean))

(defmethod exists? ((self model))
  (slot-value self 'exists?))

(defmethod model-table (self))

(defmethod model-load (self rec)
  (let* ((table (model-table self)))
    (dolist (f rec)
      (let* ((c (first f))
	     (k (name c)))
	(with-slots (column-indices) table
	  (when (slot-exists-p self k)
	    (when (gethash k column-indices)
	      (setf (slot-value self k) (column-decode c (rest f))))))))
    
    (let* ((rp (slot-value self (name self))))
      (with-slots (column-indices columns) table
	(dolist (fk (foreign-keys table))
	  (let* ((cs (columns fk)))
	    (dotimes (i (length cs))
	      (set-key rp i (assoc (aref cs i) rec))))))))
  self)

(defmethod model-store (self)
  (let* ((table (model-table self))
	 (rec (map-columns (lambda (c) (cons c (column-encode c (slot-value self (name c))))) table)))
    (dolist (fk (foreign-keys table))
      (let* ((rp (slot-value self (name fk)))
	     (cs (columns fk)))
	(dotimes (i (length cs))
	  (push (cons (aref cs i) (get-key rp i)) rec))))
    
    (with-slots (exists?) self
      (if exists?
	  (update-rec table rec)
	  (progn
	    (insert-rec table rec)
	    (setf exists? t))))))

(defclass db ()
  ((tables :initform nil :reader tables)
   (table-lookup :initform (make-hash-table) :reader table-lookup)))

(defun find-table (name &key (db *db*))
  (gethash name (table-lookup db)))

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
			      (push `(new-foreign-key ',name (find-table ',table :db self) ,@f) defs)))
			  (parse-table-form (f)
			    (ecase (kw! (first f))
			      (:column
			       (parse-column (rest f)))
			      (:foreign-key
			       (parse-foreign-key (rest f))))))
		   (dolist (tf f)
		     (parse-table-form tf)))
		 
		 (push `(let* ((table (new-table ',name '(,@key-cols) (list ,@(nreverse defs)))))
			  (push table tables)
			  (setf (gethash ',name table-lookup) table))
		       init-forms)))
	     (parse-form (f)
	       (ecase (kw! (first f))
		 (:table (parse-table (rest f))))))
      (dolist (f forms)
	(parse-form f))
      `(progn
	 (defclass ,name (db)
	   ())
	 
	 (defmethod initialize-instance :after ((self ,name) &key)
	   (with-slots (table-lookup tables) self
	     ,@(nreverse init-forms)))))))

(defun create-tables ()
  (dolist (tbl (reverse (tables *db*)))
    (table-create tbl)))

(defun drop-tables ()
  (dolist (tbl (tables *db*))
    (table-drop tbl)))

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
