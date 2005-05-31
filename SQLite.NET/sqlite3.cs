//////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2003-2004, Finisar Corporation
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//  Redistributions of source code must retain the above copyright notice,
//   this list of conditions and the following disclaimer. 
//
//  Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
//  Neither the name of the Finisar Corporation nor the names of its
//   contributors may be used to endorse or promote products derived from this
//   software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER "AS I" AND ANY EXPRESS
// OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER BE LIABLE FOR ANY DIRECT,
// INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.
//////////////////////////////////////////////////////////////////////////////
///

using System;
using System.Data;
using System.Text;
using System.Runtime.InteropServices;
using System.Globalization;

namespace Finisar.SQLite
{
	sealed internal class sqlite3 : isqlite
	{
		private Encoding		_encoding;
		private IFormatProvider	_universalProvider = CultureInfo.InvariantCulture;
		private int				_lastChangesCount = 0;
		private DateTimeFormat	_DateTimeFormat;
		private bool			_oldDateTimeFormat;

		/**
		* Using Universable Sortable format "s"
		*  - see Help documentation on DateTimeFormatInfo Class for more info.
		* In actual fact we may want to use a more compact pattern like
		* yyyyMMdd'T'HHmmss.fffffff
		* See also http://www.cl.cam.ac.uk/~mgk25/iso-time.html
		*/
		private const string ISO8601SQLiteDateTimeFormat = "yyyy-MM-dd HH:mm:ss";
		private static string[] ISO8601DateFormats = new string[] {ISO8601SQLiteDateTimeFormat,
																	  "yyyyMMddHHmmss",
																	  "yyyyMMddTHHmmssfffffff",
																	  "yyyy-MM-dd",
																	  "yy-MM-dd",
																	  "yyyyMMdd",
																	  "HH:mm:ss",
																	  "THHmmss"
															 };

		public IntPtr DB = IntPtr.Zero;

		public sqlite3( bool UTF16Encoding, DateTimeFormat dateTimeFormat, bool oldDateTimeFormat )
		{
			if( UTF16Encoding )
				_encoding = Encoding.Unicode;
			else
				_encoding = Encoding.UTF8;
			_oldDateTimeFormat = oldDateTimeFormat;
			_DateTimeFormat = dateTimeFormat;
		}

		#region Native SQLite3 functions
		/*
		** Open the sqlite database file "filename".  The "filename" is UTF-8
		** encoded for sqlite3_open() and UTF-16 encoded in the native byte order
		** for sqlite3_open16().  An sqlite3* handle is returned in *ppDb, even
		** if an error occurs. If the database is opened (or created) successfully,
		** then SQLITE_OK is returned. Otherwise an error code is returned. The
		** sqlite3_errmsg() or sqlite3_errmsg16()  routines can be used to obtain
		** an English language description of the error.
		**
		** If the database file does not exist, then a new database is created.
		** The encoding for the database is UTF-8 if sqlite3_open() is called and
		** UTF-16 if sqlite3_open16 is used.
		**
		** Whether or not an error occurs when it is opened, resources associated
		** with the sqlite3* handle should be released by passing it to
		** sqlite3_close() when it is no longer required.
		*/
		[DllImport("sqlite3")]
		private static extern int sqlite3_open(IntPtr filename, out IntPtr db);

		/*
		** Return the error code for the most recent sqlite3_* API call associated
		** with sqlite3 handle 'db'. SQLITE_OK is returned if the most recent 
		** API call was successful.
		**
		** Calls to many sqlite3_* functions set the error code and string returned
		** by sqlite3_errcode(), sqlite3_errmsg() and sqlite3_errmsg16()
		** (overwriting the previous values). Note that calls to sqlite3_errcode(),
		** sqlite3_errmsg() and sqlite3_errmsg16() themselves do not affect the
		** results of future invocations.
		**
		** Assuming no other intervening sqlite3_* API calls are made, the error
		** code returned by this function is associated with the same error as
		** the strings  returned by sqlite3_errmsg() and sqlite3_errmsg16().
		*/
		[DllImport("sqlite3")]
		private static extern int sqlite3_errcode(IntPtr db);

		/*
		** Return a pointer to a UTF-8 encoded string describing in english the
		** error condition for the most recent sqlite3_* API call. The returned
		** string is always terminated by an 0x00 byte.
		**
		** The string "not an error" is returned when the most recent API call was
		** successful.
		*/
		[DllImport("sqlite3")]
		private static extern IntPtr sqlite3_errmsg(IntPtr db);

		/*
		** Return a pointer to a UTF-16 native byte order encoded string describing
		** in english the error condition for the most recent sqlite3_* API call.
		** The returned string is always terminated by a pair of 0x00 bytes.
		**
		** The string "not an error" is returned when the most recent API call was
		** successful.
		*/
		[DllImport("sqlite3")]
		private static extern IntPtr sqlite3_errmsg16(IntPtr db);

		/*
		** A function to close the database.
		**
		** Call this function with a pointer to a structure that was previously
		** returned from sqlite3_open() and the corresponding database will by closed.
		**
		** All SQL statements prepared using sqlite3_prepare() or
		** sqlite3_prepare16() must be deallocated using sqlite3_finalize() before
		** this routine is called. Otherwise, SQLITE_BUSY is returned and the
		** database connection remains open.
		*/
		[DllImport("sqlite3")]
		private static extern int sqlite3_close(IntPtr h);

		/*
		** A function to executes one or more statements of SQL.
		**
		** If one or more of the SQL statements are queries, then
		** the callback function specified by the 3rd parameter is
		** invoked once for each row of the query result.  This callback
		** should normally return 0.  If the callback returns a non-zero
		** value then the query is aborted, all subsequent SQL statements
		** are skipped and the sqlite3_exec() function returns the SQLITE_ABORT.
		**
		** The 4th parameter is an arbitrary pointer that is passed
		** to the callback function as its first parameter.
		**
		** The 2nd parameter to the callback function is the number of
		** columns in the query result.  The 3rd parameter to the callback
		** is an array of strings holding the values for each column.
		** The 4th parameter to the callback is an array of strings holding
		** the names of each column.
		**
		** The callback function may be NULL, even for queries.  A NULL
		** callback is not an error.  It just means that no callback
		** will be invoked.
		**
		** If an error occurs while parsing or evaluating the SQL (but
		** not while executing the callback) then an appropriate error
		** message is written into memory obtained from malloc() and
		** *errmsg is made to point to that message.  The calling function
		** is responsible for freeing the memory that holds the error
		** message.   Use sqlite3_free() for this.  If errmsg==NULL,
		** then no error message is ever written.
		**
		** The return value is is SQLITE_OK if there are no errors and
		** some other return code if there is an error.  The particular
		** return value depends on the type of error. 
		**
		** If the query could not be executed because a database file is
		** locked or busy, then this function returns SQLITE_BUSY.  (This
		** behavior can be modified somewhat using the sqlite3_busy_handler()
		** and sqlite3_busy_timeout() functions below.)
		*/
		[DllImport("sqlite3")]
		private static extern int sqlite3_exec(
			IntPtr h,                      /* An open database */
			IntPtr sql,					/* SQL to be executed */
			IntPtr c,              /* Callback function */
			IntPtr arg,                       /* 1st argument to callback function */
			out IntPtr errmsg                 /* Error msg written here */
			);

		/*
		** Each entry in an SQLite table has a unique integer key.  (The key is
		** the value of the INTEGER PRIMARY KEY column if there is such a column,
		** otherwise the key is generated at random.  The unique key is always
		** available as the ROWID, OID, or _ROWID_ column.)  The following routine
		** returns the integer key of the most recent insert in the database.
		**
		** This function is similar to the mysql_insert_id() function from MySQL.
		*/
		[DllImport("sqlite3")]
		private static extern long sqlite3_last_insert_rowid(IntPtr h);

		/*
		** This function returns the number of database rows that were changed
		** (or inserted or deleted) by the most recent called sqlite3_exec().
		**
		** All changes are counted, even if they were later undone by a
		** ROLLBACK or ABORT.  Except, changes associated with creating and
		** dropping tables are not counted.
		**
		** If a callback invokes sqlite3_exec() recursively, then the changes
		** in the inner, recursive call are counted together with the changes
		** in the outer call.
		**
		** SQLite implements the command "DELETE FROM table" without a WHERE clause
		** by dropping and recreating the table.  (This is much faster than going
		** through and deleting individual elements form the table.)  Because of
		** this optimization, the change count for "DELETE FROM table" will be
		** zero regardless of the number of elements that were originally in the
		** table. To get an accurate count of the number of rows deleted, use
		** "DELETE FROM table WHERE 1" instead.
		*/
		[DllImport("sqlite3")]
		private static extern int sqlite3_changes(IntPtr h);

		/*
		** This function returns the number of database rows that have been
		** modified by INSERT, UPDATE or DELETE statements since the database handle
		** was opened. This includes UPDATE, INSERT and DELETE statements executed
		** as part of trigger programs. All changes are counted as soon as the
		** statement that makes them is completed (when the statement handle is
		** passed to sqlite3_reset() or sqlite_finalise()).
		**
		** SQLite implements the command "DELETE FROM table" without a WHERE clause
		** by dropping and recreating the table.  (This is much faster than going
		** through and deleting individual elements form the table.)  Because of
		** this optimization, the change count for "DELETE FROM table" will be
		** zero regardless of the number of elements that were originally in the
		** table. To get an accurate count of the number of rows deleted, use
		** "DELETE FROM table WHERE 1" instead.
		*/
		[DllImport("sqlite3")]
		private static extern int sqlite3_total_changes(IntPtr h);

		/*
				** This routine sets a busy handler that sleeps for a while when a
				** table is locked.  The handler will sleep multiple times until 
				** at least "ms" milleseconds of sleeping have been done.  After
				** "ms" milleseconds of sleeping, the handler returns 0 which
				** causes sqlite3_exec() to return SQLITE_BUSY.
				**
				** Calling this routine with an argument less than or equal to zero
				** turns off all busy handlers.
				*/
		[DllImport("sqlite3")]
		private static extern void sqlite3_busy_timeout(IntPtr h, int ms);

		/*
				** To execute an SQLite query without the use of callbacks, you first have
				** to compile the SQL using this routine.  The 1st parameter "db" is a pointer
				** to an sqlite object obtained from sqlite3_open().  The 2nd parameter
				** "zSql" is the text of the SQL to be compiled.   The remaining parameters
				** are all outputs.
				**
				** *pzTail is made to point to the first character past the end of the first
				** SQL statement in zSql.  This routine only compiles the first statement
				** in zSql, so *pzTail is left pointing to what remains uncompiled.
				**
				** *ppVm is left pointing to a "virtual machine" that can be used to execute
				** the compiled statement.  Or if there is an error, *ppVm may be set to NULL.
				** If the input text contained no SQL (if the input is and empty string or
				** a comment) then *ppVm is set to NULL.
				**
				** If any errors are detected during compilation, an error message is written
				** into space obtained from malloc() and *pzErrMsg is made to point to that
				** error message.  The calling routine is responsible for freeing the text
				** of this message when it has finished with it.  Use sqlite3_freemem() to
				** free the message.  pzErrMsg may be NULL in which case no error message
				** will be generated.
				**
				** On success, SQLITE_OK is returned.  Otherwise and error code is returned.
				*/
		[DllImport("sqlite3")]
		private static extern int sqlite3_prepare(
			IntPtr db,                   /* The open database */
			IntPtr zSql,             /* SQL statement to be compiled */
			int nBytes,				/* Length of zSql in bytes. */
			out IntPtr ppVm,             /* OUT: the virtual machine to execute zSql */
			out IntPtr pzTail          /* OUT: uncompiled tail of zSql */
			);


		/*
		** Return the number of columns in the result set returned by the compiled
		** SQL statement. This routine returns 0 if pStmt is an SQL statement
		** that does not return data (for example an UPDATE).
		*/
		[DllImport("sqlite3")]
		private static extern int sqlite3_column_count(IntPtr pStmt);

		/*
		** The first parameter is a compiled SQL statement. This function returns
		** the column heading for the Nth column of that statement, where N is the
		** second function parameter.  The string returned is UTF-8 for
		** sqlite3_column_name() and UTF-16 for sqlite3_column_name16().
		*/
		[DllImport("sqlite3")]
		private static extern IntPtr sqlite3_column_name(IntPtr pStmt,int iCol);
		[DllImport("sqlite3")]
		private static extern IntPtr sqlite3_column_name16(IntPtr pStmt,int iCol);

		/*
		** The first parameter is a compiled SQL statement. If this statement
		** is a SELECT statement, the Nth column of the returned result set 
		** of the SELECT is a table column then the declared type of the table
		** column is returned. If the Nth column of the result set is not at table
		** column, then a NULL pointer is returned. The returned string is always
		** UTF-8 encoded. For example, in the database schema:
		**
		** CREATE TABLE t1(c1 VARIANT);
		**
		** And the following statement compiled:
		**
		** SELECT c1 + 1, 0 FROM t1;
		**
		** Then this routine would return the string "VARIANT" for the second
		** result column (i==1), and a NULL pointer for the first result column
		** (i==0).
		*/
		[DllImport("sqlite3")]
		private static extern IntPtr sqlite3_column_decltype(IntPtr pStmt, int i);

		/*
		** The first parameter is a compiled SQL statement. If this statement
		** is a SELECT statement, the Nth column of the returned result set 
		** of the SELECT is a table column then the declared type of the table
		** column is returned. If the Nth column of the result set is not at table
		** column, then a NULL pointer is returned. The returned string is always
		** UTF-16 encoded. For example, in the database schema:
		**
		** CREATE TABLE t1(c1 INTEGER);
		**
		** And the following statement compiled:
		**
		** SELECT c1 + 1, 0 FROM t1;
		**
		** Then this routine would return the string "INTEGER" for the second
		** result column (i==1), and a NULL pointer for the first result column
		** (i==0).
		*/
		[DllImport("sqlite3")]
		private static extern IntPtr sqlite3_column_decltype16(IntPtr pStmt,int iCol);

		/* 
		** After an SQL query has been compiled with a call to either
		** sqlite3_prepare() or sqlite3_prepare16(), then this function must be
		** called one or more times to execute the statement.
		**
		** The return value will be either SQLITE_BUSY, SQLITE_DONE, 
		** SQLITE_ROW, SQLITE_ERROR, or SQLITE_MISUSE.
		**
		** SQLITE_BUSY means that the database engine attempted to open
		** a locked database and there is no busy callback registered.
		** Call sqlite3_step() again to retry the open.
		**
		** SQLITE_DONE means that the statement has finished executing
		** successfully.  sqlite3_step() should not be called again on this virtual
		** machine.
		**
		** If the SQL statement being executed returns any data, then 
		** SQLITE_ROW is returned each time a new row of data is ready
		** for processing by the caller. The values may be accessed using
		** the sqlite3_column_*() functions described below. sqlite3_step()
		** is called again to retrieve the next row of data.
		** 
		** SQLITE_ERROR means that a run-time error (such as a constraint
		** violation) has occurred.  sqlite3_step() should not be called again on
		** the VM. More information may be found by calling sqlite3_errmsg().
		**
		** SQLITE_MISUSE means that the this routine was called inappropriately.
		** Perhaps it was called on a virtual machine that had already been
		** finalized or on one that had previously returned SQLITE_ERROR or
		** SQLITE_DONE.  Or it could be the case the the same database connection
		** is being used simulataneously by two or more threads.
		*/
		[DllImport("sqlite3")]
		private static extern int sqlite3_step(IntPtr pStmt);

		/*
		** Return the number of values in the current row of the result set.
		**
		** After a call to sqlite3_step() that returns SQLITE_ROW, this routine
		** will return the same value as the sqlite3_column_count() function.
		** After sqlite3_step() has returned an SQLITE_DONE, SQLITE_BUSY or
		** error code, or before sqlite3_step() has been called on a 
		** compiled SQL statement, this routine returns zero.
		*/
		[DllImport("sqlite3")]
		private static extern int sqlite3_data_count(IntPtr pStmt);

		/*
		** Values are stored in the database in one of the following fundamental
		** types.
		*/
		public const int  SQLITE_INTEGER = 1;
		public const int  SQLITE_FLOAT   = 2;
		public const int  SQLITE_TEXT    = 3;
		public const int  SQLITE_BLOB    = 4;
		public const int  SQLITE_NULL    = 5;

		/*
		** The next group of routines returns information about the information
		** in a single column of the current result row of a query.  In every
		** case the first parameter is a pointer to the SQL statement that is being
		** executed (the sqlite_stmt* that was returned from sqlite3_prepare()) and
		** the second argument is the index of the column for which information 
		** should be returned.  iCol is zero-indexed.  The left-most column as an
		** index of 0.
		**
		** If the SQL statement is not currently point to a valid row, or if the
		** the colulmn index is out of range, the result is undefined.
		**
		** These routines attempt to convert the value where appropriate.  For
		** example, if the internal representation is FLOAT and a text result
		** is requested, sprintf() is used internally to do the conversion
		** automatically.  The following table details the conversions that
		** are applied:
		**
		**    Internal Type    Requested Type     Conversion
		**    -------------    --------------    --------------------------
		**       NULL             INTEGER         Result is 0
		**       NULL             FLOAT           Result is 0.0
		**       NULL             TEXT            Result is an empty string
		**       NULL             BLOB            Result is a zero-length BLOB
		**       INTEGER          FLOAT           Convert from integer to float
		**       INTEGER          TEXT            ASCII rendering of the integer
		**       INTEGER          BLOB            Same as for INTEGER->TEXT
		**       FLOAT            INTEGER         Convert from float to integer
		**       FLOAT            TEXT            ASCII rendering of the float
		**       FLOAT            BLOB            Same as FLOAT->TEXT
		**       TEXT             INTEGER         Use atoi()
		**       TEXT             FLOAT           Use atof()
		**       TEXT             BLOB            No change
		**       BLOB             INTEGER         Convert to TEXT then use atoi()
		**       BLOB             FLOAT           Convert to TEXT then use atof()
		**       BLOB             TEXT            Add a \000 terminator if needed
		**
		** The following access routines are provided:
		**
		** _type()     Return the datatype of the result.  This is one of
		**             SQLITE_INTEGER, SQLITE_FLOAT, SQLITE_TEXT, SQLITE_BLOB,
		**             or SQLITE_NULL.
		** _blob()     Return the value of a BLOB.
		** _bytes()    Return the number of bytes in a BLOB value or the number
		**             of bytes in a TEXT value represented as UTF-8.  The \000
		**             terminator is included in the byte count for TEXT values.
		** _bytes16()  Return the number of bytes in a BLOB value or the number
		**             of bytes in a TEXT value represented as UTF-16.  The \u0000
		**             terminator is included in the byte count for TEXT values.
		** _double()   Return a FLOAT value.
		** _int()      Return an INTEGER value in the host computer's native
		**             integer representation.  This might be either a 32- or 64-bit
		**             integer depending on the host.
		** _int64()    Return an INTEGER value as a 64-bit signed integer.
		** _text()     Return the value as UTF-8 text.
		** _text16()   Return the value as UTF-16 text.
		*/
		[DllImport("sqlite3")]
		private static extern IntPtr sqlite3_column_blob(IntPtr pStmt, int iCol);
		[DllImport("sqlite3")]
		private static extern int sqlite3_column_bytes(IntPtr pStmt, int iCol);
		[DllImport("sqlite3")]
		private static extern int sqlite3_column_bytes16(IntPtr pStmt, int iCol);
		[DllImport("sqlite3")]
		private static extern double sqlite3_column_double(IntPtr pStmt, int iCol);
		[DllImport("sqlite3_wrapper")]
		private static extern void sqlite3_column_double_ref(IntPtr pStmt, int iCol, out double val);
		[DllImport("sqlite3")]
		private static extern int sqlite3_column_int(IntPtr pStmt, int iCol);
		[DllImport("sqlite3")]
		private static extern long sqlite3_column_int64(IntPtr pStmt, int iCol);
		[DllImport("sqlite3_wrapper")]
		private static extern void sqlite3_column_int64_ref(IntPtr pStmt, int iCol, out long val);
		[DllImport("sqlite3")]
		private static extern IntPtr sqlite3_column_text(IntPtr pStmt, int iCol);
		[DllImport("sqlite3")]
		private static extern IntPtr sqlite3_column_text16(IntPtr pStmt, int iCol);
		[DllImport("sqlite3")]
		private static extern int sqlite3_column_type(IntPtr pStmt, int iCol);

		/*
				** This routine is called to delete a virtual machine after it has finished
				** executing.  The return value is the result code.  SQLITE_OK is returned
				** if the statement executed successfully and some other value is returned if
				** there was any kind of error.  If an error occurred and pzErrMsg is not
				** NULL, then an error message is written into memory obtained from malloc()
				** and *pzErrMsg is made to point to that error message.  The calling routine
				** should use sqlite3_freemem() to delete this message when it has finished
				** with it.
				**
				** This routine can be called at any point during the execution of the
				** virtual machine.  If the virtual machine has not completed execution
				** when this routine is called, that is like encountering an error or
				** an interrupt.  (See sqlite3_interrupt().)  Incomplete updates may be
				** rolled back and transactions cancelled,  depending on the circumstances,
				** and the result code returned will be SQLITE_ABORT.
				*/
		[DllImport("sqlite3")]
		private static extern int sqlite3_finalize(IntPtr h);

		/*
				** This routine deletes the virtual machine, writes any error message to
				** *pzErrMsg and returns an SQLite return code in the same way as the
				** sqlite3_finalize() function.
				**
				** Additionally, if ppVm is not NULL, *ppVm is left pointing to a new virtual
				** machine loaded with the compiled version of the original query ready for
				** execution.
				**
				** If sqlite3_reset() returns SQLITE_SCHEMA, then *ppVm is set to NULL.
				**
				******* THIS IS AN EXPERIMENTAL API AND IS SUBJECT TO CHANGE ******
				*/
		[DllImport("sqlite3")]
		private static extern int sqlite3_reset(IntPtr h);

		/*
		** These are special value for the destructor that is passed in as the
		** final argument to routines like sqlite3_result_blob().  If the destructor
		** argument is SQLITE_STATIC, it means that the content pointer is constant
		** and will never change.  It does not need to be destroyed.  The 
		** SQLITE_TRANSIENT value means that the content will likely change in
		** the near future and that SQLite should make its own private copy of
		** the content before returning.
		*/
		private const int SQLITE_STATIC = 0;
		private const int SQLITE_TRANSIENT = -1;

		/*
		** In the SQL strings input to sqlite3_prepare() and sqlite3_prepare16(),
		** one or more literals can be replace by a wildcard "?" or ":N:" where
		** N is an integer.  These value of these wildcard literals can be set
		** using the routines listed below.
		**
		** In every case, the first parameter is a pointer to the sqlite3_stmt
		** structure returned from sqlite3_prepare().  The second parameter is the
		** index of the wildcard.  The first "?" has an index of 1.  ":N:" wildcards
		** use the index N.
		**
		** The fifth parameter to sqlite3_bind_blob(), sqlite3_bind_text(), and
		** sqlite3_bind_text16() is a destructor used to dispose of the BLOB or
		** text after SQLite has finished with it.  If the fifth argument is the
		** special value SQLITE_STATIC, then the library assumes that the information
		** is in static, unmanaged space and does not need to be freed.  If the
		** fifth argument has the value SQLITE_TRANSIENT, then SQLite makes its
		** own private copy of the data.
		**
		** The sqlite3_bind_* routine must be called before sqlite3_step() after
		** an sqlite3_prepare() or sqlite3_reset().  Unbound wildcards are interpreted
		** as NULL.
		*/
		[DllImport("sqlite3")]
		private unsafe static extern int sqlite3_bind_blob(IntPtr stmt, int idx, byte *val, int n, int destructor );
		[DllImport("sqlite3")]
		private static extern int sqlite3_bind_double(IntPtr stmt, int idx, double val);
		[DllImport("sqlite3_wrapper")]
		private static extern int sqlite3_bind_double_ref(IntPtr stmt, int idx, [In] ref double val);
		[DllImport("sqlite3")]
		private static extern int sqlite3_bind_int(IntPtr stmt, int idx, int val);
		[DllImport("sqlite3")]
		private static extern int sqlite3_bind_int64(IntPtr stmt, int idx, long val);
		[DllImport("sqlite3_wrapper")]
		private static extern int sqlite3_bind_int64_ref(IntPtr stmt, int idx, [In] ref long val);
		[DllImport("sqlite3")]
		private static extern int sqlite3_bind_null(IntPtr stmt, int idx);
		[DllImport("sqlite3")]
		private static extern int sqlite3_bind_text(IntPtr stmt, int idx, IntPtr val, int n, int destructor );
		[DllImport("sqlite3")]
		private static extern int sqlite3_bind_text16(IntPtr stmt, int idx, IntPtr val, int n, int destructor );
		#endregion

		#region isqlite Members

		public void open(string filename)
		{
			using( MarshalStr m = new MarshalStr(_encoding,filename) )
			{
				int res = sqlite3_open(m.GetSQLiteStr(),out DB);
				if( res != SQLITE.OK )
				{
					try
					{
						CheckOK();
					}
					finally
					{
						sqlite3_close(DB);
						DB = IntPtr.Zero;
					}
				}
			}
		}

		public void close()
		{
			if( DB != IntPtr.Zero )
			{
				sqlite3_close(DB);
				DB = IntPtr.Zero;
			}
		}

		public void exec(string sql)
		{
			using( MarshalStr m = new MarshalStr(sql) )
			{
				IntPtr errmsg;
				sqlite3_exec(DB,m.GetSQLiteStr(),IntPtr.Zero,IntPtr.Zero,out errmsg);
				CheckOK();
			}
		}

		public int last_statement_changes()
		{
			return _lastChangesCount;
		}

		public void busy_timeout(int ms)
		{
			sqlite3_busy_timeout(DB, ms);
		}

		public isqlite_vm compile(string zSql)
		{
			using( MarshalStr s = new MarshalStr(_encoding,zSql) )
			{
				IntPtr pTail;
				IntPtr pVM;
				sqlite3_prepare(DB,s.GetSQLiteStr(),s.GetSQLiteStrByteLength(),out pVM,out pTail);
				CheckOK();
				return new sqlite_vm(this,pVM);
			}
		}

		#endregion

		public long get_last_insert_rowid()
		{
			return sqlite3_last_insert_rowid(DB);
		}

		#region Helper private functions 

		private string convert( IntPtr s )
		{
			return MarshalStr.FromSQLite (s,_encoding);
		}

		private void CheckOK ()
		{
			if (sqlite3_errcode(DB) != SQLITE.OK)
				Throw();
		}

		private void Throw ()
		{
			throw new SQLiteException(MarshalStr.FromSQLite(sqlite3_errmsg(DB),Encoding.UTF8));
		}
		#endregion

		#region Implementation of isqlite_vm
		private class sqlite_vm : isqlite_vm
		{
			public IntPtr	Handle;
			private sqlite3 _connection;
			private bool	_firstStep = true;
			private int		_previousTotalChangesCount;

			public sqlite_vm( sqlite3 sq, IntPtr handle )
			{
				_connection = sq;
				Handle = handle;
			}

			#region isqlite_vm Members

			public int step()
			{
				if( _firstStep )
				{
					_firstStep = false;
					_previousTotalChangesCount = sqlite3_total_changes(_connection.DB);
				}
				int res = sqlite3_step(Handle);
				if (res == SQLITE.MISUSE || res == SQLITE.BUSY)
					_connection.Throw();
				return res;
			}

			public void Dispose()
			{
				if( Handle != IntPtr.Zero )
				{
					int res = sqlite3_finalize(Handle);
					Handle = IntPtr.Zero;
					if (res != SQLITE.OK && res != SQLITE.ABORT)
						_connection.Throw();
					_connection._lastChangesCount = sqlite3_total_changes(_connection.DB) - _previousTotalChangesCount;
				}
			}

			public void reset()
			{
				sqlite3_reset(Handle);
				_connection.CheckOK();
				_firstStep = true;
				_connection._lastChangesCount = sqlite3_total_changes(_connection.DB) - _previousTotalChangesCount;
			}

			public void bind(int idx, IDbDataParameter p )
			{
				object aVal = p.Value;
				if( aVal == null || aVal == DBNull.Value )
					sqlite3_bind_null(Handle,idx);
				else
				{
					switch( p.DbType )
					{
						case DbType.Boolean:
							sqlite3_bind_int(Handle,idx,Convert.ToBoolean(aVal)?1:0);
							break;

						case DbType.Byte:
						case DbType.Int16:
						case DbType.Int32:
						case DbType.SByte:
						case DbType.UInt16:
						case DbType.UInt32:
							sqlite3_bind_int(Handle,idx,Convert.ToInt32(aVal));
							break;
							
						case DbType.Double:
						case DbType.Single:
							bind_double(idx,Convert.ToDouble(aVal));
							break;

						case DbType.Date:
						case DbType.DateTime:
						case DbType.Time:
							if( _connection._oldDateTimeFormat )
								goto default;
							else if ( _connection._DateTimeFormat.Equals(DateTimeFormat.CurrentCulture ) )
							{
								aVal = Convert.ToDateTime(aVal).ToString(DateTimeFormatInfo.CurrentInfo);
								goto default;
							}
							else if ( _connection._DateTimeFormat.Equals(DateTimeFormat.Ticks ) )
							{
								bind_long(idx,Convert.ToDateTime(aVal).Ticks);
							}
							else
							{
								aVal = Convert.ToDateTime(aVal).ToString(ISO8601SQLiteDateTimeFormat);
								goto default;
							}

							break;

						case DbType.Int64:
						case DbType.UInt64:
							bind_long(idx,Convert.ToInt64(aVal));
							break;

						case DbType.Binary:
							unsafe
							{
								byte[] bytes = (byte[])aVal;
								fixed( byte *b = bytes )
								{
									sqlite3_bind_blob(Handle,idx,b,bytes.Length,SQLITE_TRANSIENT);
								}
							}
							break;

						default:
							MarshalStr pStr = ((SQLiteParameter)p).GetMarshalStr(_connection._encoding);
							if( pStr.Str == null )
							{
								// cache was purged, fill it again
								string s = null;
								switch( p.DbType )
								{
									case DbType.Currency:
									case DbType.Decimal:
										s = Convert.ToDecimal(aVal).ToString(_connection._universalProvider);
										break;

									default:
										s = Convert.ToString(aVal);
										break;
								}
								pStr.Str = s;
							}

							IntPtr val = pStr.GetSQLiteStr();
							int len = -1; //pStr.GetSQLiteStrByteLength()-1;
							sqlite3_bind_text(Handle,idx,val,len,SQLITE_STATIC);
							break;
					}
				}
				_connection.CheckOK();
			}

			private void bind_double( int idx, double val )
			{
#if PLATFORM_COMPACTFRAMEWORK
				sqlite3_bind_double_ref(Handle,idx,ref val);
#else
				sqlite3_bind_double(Handle,idx,val);
#endif
			}

			private void bind_long( int idx, long val )
			{
#if PLATFORM_COMPACTFRAMEWORK
				sqlite3_bind_int64_ref(Handle,idx,ref val);
#else
				sqlite3_bind_int64(Handle,idx,val);
#endif
			}

			public int GetColumnCount()
			{
				return sqlite3_column_count(Handle);
			}

			public string GetColumnName(int iCol)
			{
				return _connection.convert(sqlite3_column_name(Handle,iCol));
			}

			public string GetColumnType(int iCol)
			{
				string type = _connection.convert(sqlite3_column_decltype(Handle,iCol));
				if ( type == null ) 
				{
					// Try a different method for determining the column type.
					switch( sqlite3_column_type(Handle,iCol) ) 
					{
						case SQLITE_INTEGER:
							type = "INT64";
							break;
						case SQLITE_FLOAT:
							type = "FLOAT";
							break;
						case SQLITE_TEXT:
							type = "TEXT";
							break;
						case SQLITE_BLOB:
							type = "BLOB";
							break;
//						case SQLITE_NULL:
//							break;
					}
				}

				return type;
			}

			public bool	IsColumnNull(int iCol)
			{
				return sqlite3_column_type(Handle,iCol) == SQLITE_NULL;
			}

			public bool IsColumnEmptyString(int iCol)
			{
				if( sqlite3_column_type(Handle,iCol) == SQLITE_TEXT )
				{
					int bytes = sqlite3_column_bytes(Handle,iCol);
					return bytes <= 1;
				}
				return false;
			}

			public string GetColumnValue(int iCol)
			{
				IntPtr fieldValue = sqlite3_column_text(Handle,iCol);
				return _connection.convert(fieldValue);
			}

			public int GetColumnInt(int iCol)
			{
				return sqlite3_column_int(Handle,iCol);
			}

			public long GetColumnLong(int iCol)
			{
#if PLATFORM_COMPACTFRAMEWORK
				long val;
				sqlite3_column_int64_ref(Handle,iCol,out val);
				return val;
#else
				return sqlite3_column_int64(Handle,iCol);
#endif
			}

			public double GetColumnDouble(int iCol)
			{
#if PLATFORM_COMPACTFRAMEWORK
				double val;
				sqlite3_column_double_ref(Handle,iCol,out val);
				return val;
#else
				return sqlite3_column_double(Handle,iCol);
#endif
			}

			public decimal GetColumnDecimal(int iCol)
			{
				return decimal.Parse(GetColumnValue(iCol),_connection._universalProvider);
			}

			public DateTime GetColumnDateTime(int iCol)
			{
				if( _connection._oldDateTimeFormat || _connection._DateTimeFormat.Equals(DateTimeFormat.CurrentCulture ) ) 
				{
					return DateTime.Parse(GetColumnValue(iCol));
				}
				else if ( _connection._DateTimeFormat.Equals(DateTimeFormat.Ticks ) )
				{
					return new DateTime(GetColumnLong(iCol));
				}
				else
				{
					string val = GetColumnValue(iCol);
					object retVal = null;
					try 
					{
						retVal = DateTime.ParseExact(val, ISO8601DateFormats, 
														DateTimeFormatInfo.InvariantInfo,
														DateTimeStyles.None);
					} 
					catch {}
					if ( retVal == null ) 
					{
						try 
						{
							long iVal = Int64.Parse(val);

							// Try the old Ticks format.
							retVal = new DateTime(iVal);
						} 
						catch
						{
							throw new SQLiteException("Invalid DateTime Field Format: " + val);
						}
					}
					return (DateTime)retVal;
				}
			}

			public long GetColumnBytes(int iCol, long fieldOffset, Byte [] buffer, int bufferoffset, int length )
			{
				int fieldLen = sqlite3_column_bytes(Handle,iCol);
				if (buffer == null)
					return fieldLen;

				IntPtr s = sqlite3_column_blob(Handle,iCol);
				if( s == IntPtr.Zero )
					return 0;

				IntPtr src = (IntPtr)(s.ToInt32() + fieldOffset);
				if ((fieldOffset + length) > fieldLen)
					length = fieldLen - (int) fieldOffset;
				if ((bufferoffset + length) > buffer.Length)
					length = buffer.Length - bufferoffset;
				Marshal.Copy (src, buffer, bufferoffset, length);
				return length;
			}

			#endregion
		}

		#endregion
	}
}
