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
using System.Collections;
using System.Globalization;

namespace Finisar.SQLite
{
	sealed internal class sqlite2 : isqlite
	{
		private Encoding		_encoding;
		private IFormatProvider	_universalProvider = CultureInfo.InvariantCulture;
		private bool			_oldDateTimeFormat;
		private bool			_oldBinaryFormat;
		private DateTimeFormat	_DateTimeFormat;

		public  IntPtr   DB = IntPtr.Zero;

		public sqlite2( bool UTF8Encoding, DateTimeFormat dateTimeFormat, bool oldDateTimeFormat, bool oldBinaryFormat )
		{
			if( UTF8Encoding )
				_encoding = Encoding.UTF8;
			else
				_encoding = Encoding.ASCII;
			_DateTimeFormat = dateTimeFormat;
			_oldDateTimeFormat = oldDateTimeFormat;
			_oldBinaryFormat = oldBinaryFormat;
		}

		#region Native SQLite functions
		/*
				** A function to open a new sqlite database.  
				**
				** If the database does not exist and mode indicates write
				** permission, then a new database is created.  If the database
				** does not exist and mode does not indicate write permission,
				** then the open fails, an error message generated (if errmsg!=0)
				** and the function returns 0.
				** 
				** If mode does not indicates user write permission, then the 
				** database is opened read-only.
				**
				** The Truth:  As currently implemented, all databases are opened
				** for writing all the time.  Maybe someday we will provide the
				** ability to open a database readonly.  The mode parameters is
				** provided in anticipation of that enhancement.
				*/
		[DllImport("sqlite")]
		private static extern IntPtr sqlite_open(IntPtr filename, int mode, out IntPtr errmsg);

		/*
				** A function to close the database.
				**
				** Call this function with a pointer to a structure that was previously
				** returned from sqlite_open() and the corresponding database will by closed.
				*/
		[DllImport("sqlite")]
		private static extern void sqlite_close(IntPtr h);

		/*
				** A function to executes one or more statements of SQL.
				**
				** If one or more of the SQL statements are queries, then
				** the callback function specified by the 3rd parameter is
				** invoked once for each row of the query result.  This callback
				** should normally return 0.  If the callback returns a non-zero
				** value then the query is aborted, all subsequent SQL statements
				** are skipped and the sqlite_exec() function returns the SQLITE_ABORT.
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
				** message.   Use sqlite_freemem() for this.  If errmsg==NULL,
				** then no error message is ever written.
				**
				** The return value is is SQLITE_OK if there are no errors and
				** some other return code if there is an error.  The particular
				** return value depends on the type of error. 
				**
				** If the query could not be executed because a database file is
				** locked or busy, then this function returns SQLITE_BUSY.  (This
				** behavior can be modified somewhat using the sqlite_busy_handler()
				** and sqlite_busy_timeout() functions below.)
				*/
		[DllImport("sqlite")]
		private static extern int sqlite_exec(
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
		[DllImport("sqlite")]
		private static extern int sqlite_last_insert_rowid(IntPtr h);

		/*
				** This function returns the number of database rows that were changed
				** (or inserted or deleted) by the most recent called sqlite_exec().
				**
				** All changes are counted, even if they were later undone by a
				** ROLLBACK or ABORT.  Except, changes associated with creating and
				** dropping tables are not counted.
				**
				** If a callback invokes sqlite_exec() recursively, then the changes
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
		[DllImport("sqlite")]
		private static extern int sqlite_changes(IntPtr h);

		/*
				** This function returns the number of database rows that were changed
				** by the last INSERT, UPDATE, or DELETE statment executed by sqlite_exec(),
				** or by the last VM to run to completion. The change count is not updated
				** by SQL statements other than INSERT, UPDATE or DELETE.
				**
				** Changes are counted, even if they are later undone by a ROLLBACK or
				** ABORT. Changes associated with trigger programs that execute as a
				** result of the INSERT, UPDATE, or DELETE statement are not counted.
				**
				** If a callback invokes sqlite_exec() recursively, then the changes
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
				**
				******* THIS IS AN EXPERIMENTAL API AND IS SUBJECT TO CHANGE ******
				*/
		[DllImport("sqlite")]
		private static extern int sqlite_last_statement_changes(IntPtr h);

		/* If the parameter to this routine is one of the return value constants
				** defined above, then this routine returns a constant text string which
				** descripts (in English) the meaning of the return value.
				*/
		[DllImport("sqlite")]
		private static extern IntPtr sqlite_error_string(int n);

		/*
				** This routine sets a busy handler that sleeps for a while when a
				** table is locked.  The handler will sleep multiple times until 
				** at least "ms" milleseconds of sleeping have been done.  After
				** "ms" milleseconds of sleeping, the handler returns 0 which
				** causes sqlite_exec() to return SQLITE_BUSY.
				**
				** Calling this routine with an argument less than or equal to zero
				** turns off all busy handlers.
				*/
		[DllImport("sqlite")]
		private static extern void sqlite_busy_timeout(IntPtr h, int ms);

		/*
				** Windows systems should call this routine to free memory that
				** is returned in the in the errmsg parameter of sqlite_open() when
				** SQLite is a DLL.  For some reason, it does not work to call free()
				** directly.
				*/
		[DllImport("sqlite")]
		private static extern void sqlite_freemem(IntPtr p);

		/*
				** To execute an SQLite query without the use of callbacks, you first have
				** to compile the SQL using this routine.  The 1st parameter "db" is a pointer
				** to an sqlite object obtained from sqlite_open().  The 2nd parameter
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
				** of this message when it has finished with it.  Use sqlite_freemem() to
				** free the message.  pzErrMsg may be NULL in which case no error message
				** will be generated.
				**
				** On success, SQLITE_OK is returned.  Otherwise and error code is returned.
				*/
		[DllImport("sqlite")]
		private static extern int sqlite_compile(
			IntPtr db,                   /* The open database */
			IntPtr zSql,             /* SQL statement to be compiled */
			out IntPtr pzTail,          /* OUT: uncompiled tail of zSql */
			out IntPtr ppVm,             /* OUT: the virtual machine to execute zSql */
			out IntPtr pzErrmsg               /* OUT: Error message. */
			);

		/*
				** After an SQL statement has been compiled, it is handed to this routine
				** to be executed.  This routine executes the statement as far as it can
				** go then returns.  The return value will be one of SQLITE_DONE,
				** SQLITE_ERROR, SQLITE_BUSY, SQLITE_ROW, or SQLITE_MISUSE.
				**
				** SQLITE_DONE means that the execute of the SQL statement is complete
				** an no errors have occurred.  sqlite_step() should not be called again
				** for the same virtual machine.  *pN is set to the number of columns in
				** the result set and *pazColName is set to an array of strings that
				** describe the column names and datatypes.  The name of the i-th column
				** is (*pazColName)[i] and the datatype of the i-th column is
				** (*pazColName)[i+*pN].  *pazValue is set to NULL.
				**
				** SQLITE_ERROR means that the virtual machine encountered a run-time
				** error.  sqlite_step() should not be called again for the same
				** virtual machine.  *pN is set to 0 and *pazColName and *pazValue are set
				** to NULL.  Use sqlite_finalize() to obtain the specific error code
				** and the error message text for the error.
				**
				** SQLITE_BUSY means that an attempt to open the database failed because
				** another thread or process is holding a lock.  The calling routine
				** can try again to open the database by calling sqlite_step() again.
				** The return code will only be SQLITE_BUSY if no busy handler is registered
				** using the sqlite_busy_handler() or sqlite_busy_timeout() routines.  If
				** a busy handler callback has been registered but returns 0, then this
				** routine will return SQLITE_ERROR and sqltie_finalize() will return
				** SQLITE_BUSY when it is called.
				**
				** SQLITE_ROW means that a single row of the result is now available.
				** The data is contained in *pazValue.  The value of the i-th column is
				** (*azValue)[i].  *pN and *pazColName are set as described in SQLITE_DONE.
				** Invoke sqlite_step() again to advance to the next row.
				**
				** SQLITE_MISUSE is returned if sqlite_step() is called incorrectly.
				** For example, if you call sqlite_step() after the virtual machine
				** has halted (after a prior call to sqlite_step() has returned SQLITE_DONE)
				** or if you call sqlite_step() with an incorrectly initialized virtual
				** machine or a virtual machine that has been deleted or that is associated
				** with an sqlite structure that has been closed.
				*/
		[DllImport("sqlite")]
		private static extern int sqlite_step(
			IntPtr pVm,              /* The virtual machine to execute */
			out int pN,                     /* OUT: Number of columns in result */
			out IntPtr pazValue,      /* OUT: Column data */
			out IntPtr pazColName     /* OUT: Column names and datatypes */
			);

		/*
				** This routine is called to delete a virtual machine after it has finished
				** executing.  The return value is the result code.  SQLITE_OK is returned
				** if the statement executed successfully and some other value is returned if
				** there was any kind of error.  If an error occurred and pzErrMsg is not
				** NULL, then an error message is written into memory obtained from malloc()
				** and *pzErrMsg is made to point to that error message.  The calling routine
				** should use sqlite_freemem() to delete this message when it has finished
				** with it.
				**
				** This routine can be called at any point during the execution of the
				** virtual machine.  If the virtual machine has not completed execution
				** when this routine is called, that is like encountering an error or
				** an interrupt.  (See sqlite_interrupt().)  Incomplete updates may be
				** rolled back and transactions cancelled,  depending on the circumstances,
				** and the result code returned will be SQLITE_ABORT.
				*/
		[DllImport("sqlite")]
		private static extern int sqlite_finalize(IntPtr h, out IntPtr pzErrMsg);

		/*
				** This routine deletes the virtual machine, writes any error message to
				** *pzErrMsg and returns an SQLite return code in the same way as the
				** sqlite_finalize() function.
				**
				** Additionally, if ppVm is not NULL, *ppVm is left pointing to a new virtual
				** machine loaded with the compiled version of the original query ready for
				** execution.
				**
				** If sqlite_reset() returns SQLITE_SCHEMA, then *ppVm is set to NULL.
				**
				******* THIS IS AN EXPERIMENTAL API AND IS SUBJECT TO CHANGE ******
				*/
		[DllImport("sqlite")]
		private static extern int sqlite_reset(IntPtr h, out IntPtr pzErrMsg);

		/*
				** If the SQL that was handed to sqlite_compile contains variables that
				** are represeted in the SQL text by a question mark ('?').  This routine
				** is used to assign values to those variables.
				**
				** The first parameter is a virtual machine obtained from sqlite_compile().
				** The 2nd "idx" parameter determines which variable in the SQL statement
				** to bind the value to.  The left most '?' is 1.  The 3rd parameter is
				** the value to assign to that variable.  The 4th parameter is the number
				** of bytes in the value, including the terminating \000 for strings.
				** Finally, the 5th "copy" parameter is TRUE if SQLite should make its
				** own private copy of this value, or false if the space that the 3rd
				** parameter points to will be unchanging and can be used directly by
				** SQLite.
				**
				** Unbound variables are treated as having a value of NULL.  To explicitly
				** set a variable to NULL, call this routine with the 3rd parameter as a
				** NULL pointer.
				**
				** If the 4th "len" parameter is -1, then strlen() is used to find the
				** length.
				**
				** This routine can only be called immediately after sqlite_compile()
				** or sqlite_reset() and before any calls to sqlite_step().
				**
				******* THIS IS AN EXPERIMENTAL API AND IS SUBJECT TO CHANGE ******
				*/
		[DllImport("sqlite")]
		private static extern int sqlite_bind(IntPtr h, int idx, IntPtr val, int len, int copy);

		/*
		** Encode a binary buffer "in" of size n bytes so that it contains
		** no instances of characters '\'' or '\000'.  The output is 
		** null-terminated and can be used as a string value in an INSERT
		** or UPDATE statement.  Use sqlite_decode_binary() to convert the
		** string back into its original binary.
		**
		** The result is written into a preallocated output buffer "out".
		** "out" must be able to hold at least 2 +(257*n)/254 bytes.
		** In other words, the output will be expanded by as much as 3
		** bytes for every 254 bytes of input plus 2 bytes of fixed overhead.
		** (This is approximately 2 + 1.0118*n or about a 1.2% size increase.)
		**
		** The return value is the number of characters in the encoded
		** string, excluding the "\000" terminator.
		**
		** If out==NULL then no output is generated but the routine still returns
		** the number of characters that would have been generated if out had
		** not been NULL.
		*/
		[DllImport("sqlite")]
		private static extern int sqlite_encode_binary( IntPtr inPtr, int n, IntPtr outPtr );

		/*
		** Decode the string "in" into binary data and write it into "out".
		** This routine reverses the encoding created by sqlite_encode_binary().
		** The output will always be a few bytes less than the input.  The number
		** of bytes of output is returned.  If the input is not a well-formed
		** encoding, -1 is returned.
		**
		** The "in" and "out" parameters may point to the same buffer in order
		** to decode a string in place.
		*/
		[DllImport("sqlite")]
		private static extern int sqlite_decode_binary( IntPtr inPtr, IntPtr outPtr );
		#endregion

		#region isqlite Members

		public void open(string filename)
		{
			using( MarshalStr m = new MarshalStr(filename) )
			{
				IntPtr perr;
				IntPtr handle = sqlite_open(m.GetSQLiteStr(),0,out perr);
				if( handle == IntPtr.Zero )
					Throw (SQLITE.ERROR, perr);
				DB = handle;
			}
		}

		public void close()
		{
			if( DB != IntPtr.Zero )
			{
				sqlite_close(DB);
				DB = IntPtr.Zero;
			}
		}

		public void exec(string sql)
		{
			using( MarshalStr m = new MarshalStr(sql) )
			{
				IntPtr errmsg;
				int result = sqlite_exec(DB,m.GetSQLiteStr(),IntPtr.Zero,IntPtr.Zero,out errmsg);
				CheckOK(result,errmsg);
			}
		}

		public int last_statement_changes()
		{
			return sqlite_last_statement_changes(DB);
		}

		public void busy_timeout(int ms)
		{
			sqlite_busy_timeout(DB, ms);
		}

		public isqlite_vm compile(string zSql)
		{
			using( MarshalStr s = new MarshalStr(_encoding,zSql) )
			{
				IntPtr pzErrmsg;
				IntPtr pTail;
				IntPtr pVm;
				int result = sqlite_compile(DB,s.GetSQLiteStr(),out pTail,out pVm,out pzErrmsg);
				CheckOK(result,pzErrmsg);
				return new sqlite_vm(this,pVm);
			}
		}

		#endregion

		#region Helper private functions 

		private void CheckOK (int sqliteResult)
		{
			CheckOK (sqliteResult, IntPtr.Zero);
		}

		private void CheckOK (int sqliteResult, IntPtr pSQLiteErrMsg)
		{
			if (sqliteResult != SQLITE.OK)
				Throw(sqliteResult,pSQLiteErrMsg);
		}

		private void Throw (int sqliteResult)
		{
			Throw (sqliteResult, IntPtr.Zero);
		}

		private void Throw (int sqliteResult, IntPtr pSQLiteErrMsg)
		{
			StringBuilder pMsg = new StringBuilder (MarshalStr.FromSQLite(sqlite_error_string (sqliteResult),Encoding.ASCII));
			if (pSQLiteErrMsg != IntPtr.Zero)
			{
				pMsg.Append (":  ");
				pMsg.Append (MarshalStr.FromSQLite(pSQLiteErrMsg,Encoding.ASCII));
				sqlite_freemem(pSQLiteErrMsg);
			}
			throw new SQLiteException (pMsg.ToString());
		}
		#endregion

		#region Implementation of isqlite_vm
		private class sqlite_vm : isqlite_vm
		{
			public IntPtr	Handle;
			public int		N;
			public IntPtr	Value;
			public IntPtr	ColName;
			private sqlite2	_connection;

			private const string NewDateTimeFormat = "/0000000000000000000";

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

			public sqlite_vm( sqlite2 conn, IntPtr handle )
			{
				_connection = conn;
				Handle = handle;
			}

			#region isqlite_vm Members

			public int step()
			{
				int res = sqlite_step(Handle,out N,out Value,out ColName);
				if (res == SQLITE.MISUSE || res == SQLITE.BUSY)
					_connection.Throw(res);
				return res;
			}

			public void Dispose()
			{
				if( Handle != IntPtr.Zero )
				{
					IntPtr pzErrMsg;
					int res = sqlite_finalize(Handle,out pzErrMsg);
					Handle = IntPtr.Zero;
					if (res != SQLITE.OK && res != SQLITE.ABORT)
						_connection.Throw(res, pzErrMsg);
				}
			}

			public void reset()
			{
				IntPtr pzErrMsg;
				int result = sqlite_reset(Handle,out pzErrMsg);
				_connection.CheckOK(result,pzErrMsg);
			}

			public void bind(int idx, IDbDataParameter p )
			{
				int res = 0;
				object aVal = p.Value;
				if( aVal == null || aVal == DBNull.Value )
					res = sqlite_bind(Handle,idx,IntPtr.Zero,0,0);
				else if( p.DbType == DbType.Binary || aVal.GetType() == typeof(Byte[]) )
				{
					Byte[] bytes = (Byte[])aVal;
					if( _connection._oldBinaryFormat )
						res = bind_old_binary(idx,bytes);
					else
						res = bind_new_binary(idx,bytes);
				}
				else
				{
					MarshalStr pStr = ((SQLiteParameter)p).GetMarshalStr(_connection._encoding);
					if( pStr.Str == null )
					{
						// cache was purged, fill it again
						string s;
						if( p.DbType == DbType.Double || aVal is Double )
							s = Convert.ToDouble(aVal).ToString("R",_connection._universalProvider);
						else if( p.DbType == DbType.Single || aVal is Single )
							s = Convert.ToSingle(aVal).ToString("R",_connection._universalProvider);
						else if( p.DbType == DbType.Boolean || aVal is Boolean )
							s = Convert.ToBoolean(aVal) ? "1" : "0";
						else if( p.DbType == DbType.Currency || p.DbType == DbType.Decimal || aVal is Decimal )
							s = Convert.ToDecimal(aVal).ToString(_connection._universalProvider);
						else if( p.DbType == DbType.Date || p.DbType == DbType.DateTime || p.DbType == DbType.Time || aVal is DateTime )
						{
							if( _connection._oldDateTimeFormat )
								s = Convert.ToString(aVal);
							else if( _connection._DateTimeFormat.Equals( DateTimeFormat.ISO8601 ) )
							{
								/**
									* Using Universable Sortable format "s"
									*  - see Help documentation on DateTimeFormatInfo Class for more info.
									* In actual fact we may want to use a more compact pattern like
									* yyyyMMdd'T'HHmmss.fffffff
									* See also http://www.cl.cam.ac.uk/~mgk25/iso-time.html
									*/
								s = Convert.ToDateTime(aVal).ToString(ISO8601SQLiteDateTimeFormat);
							}
							else
								s = Convert.ToDateTime(aVal).Ticks.ToString(NewDateTimeFormat);
						}
						else
							s = Convert.ToString(aVal);
						pStr.Str = s;
					}

					IntPtr val = pStr.GetSQLiteStr();
					int len = pStr.GetSQLiteStrByteLength();
					int copy = 0;
					res = sqlite_bind(Handle,idx,val,len,copy);
				}
				_connection.CheckOK(res);
			}

			public int GetColumnCount()
			{
				return N;
			}

			public string GetColumnName(int iCol)
			{
				IntPtr fieldName = (IntPtr)Marshal.ReadInt32(ColName,iCol*IntPtr.Size);
				return MarshalStr.FromSQLite(fieldName,_connection._encoding);
			}

			public string GetColumnType(int iCol)
			{
				IntPtr fieldType = (IntPtr)Marshal.ReadInt32(ColName,(iCol+N)*IntPtr.Size);
				return MarshalStr.FromSQLite(fieldType,_connection._encoding);
			}

			public bool	IsColumnNull(int iCol)
			{
				return GetColumnPtr(iCol) == IntPtr.Zero;
			}

			public bool IsColumnEmptyString(int iCol)
			{
				IntPtr ptr = GetColumnPtr(iCol);
				return Marshal.ReadByte(ptr) == 0;
			}

			public string GetColumnValue(int iCol)
			{
				IntPtr fieldValue = GetColumnPtr(iCol);
				return MarshalStr.FromSQLite(fieldValue,_connection._encoding);
			}

			public int GetColumnInt(int iCol)
			{
				return int.Parse(GetColumnValue(iCol));
			}

			public long GetColumnLong(int iCol)
			{
				return long.Parse(GetColumnValue(iCol));
			}

			public double GetColumnDouble(int iCol)
			{
				return double.Parse(GetColumnValue(iCol),_connection._universalProvider);
			}

			public decimal GetColumnDecimal(int iCol)
			{
				return decimal.Parse(GetColumnValue(iCol),_connection._universalProvider);
			}

			public DateTime GetColumnDateTime(int iCol)
			{
				string s = GetColumnValue(iCol);
				if( _connection._oldDateTimeFormat || _connection._DateTimeFormat.Equals( DateTimeFormat.CurrentCulture ) )
					return DateTime.Parse(s);
				else if ( _connection._DateTimeFormat.Equals( DateTimeFormat.ISO8601 ) ) 
				{
					return DateTime.ParseExact(s, ISO8601DateFormats, DateTimeFormatInfo.InvariantInfo,
						DateTimeStyles.None);
				}
				else
				{
					if( s.Length <= 0 || s[0] != NewDateTimeFormat[0] )
						throw new FormatException("DateTime field in the database doesn't adhere to the format:"+NewDateTimeFormat);
					long ticks = long.Parse(s.Substring(1),NumberStyles.Integer);
					return new DateTime(ticks);
				}
			}

			public long GetColumnBytes(int iCol, long fieldOffset, Byte [] buffer, int bufferoffset, int length )
			{
				IntPtr tempBuffer = IntPtr.Zero;

				IntPtr s = GetColumnPtr(iCol);
				if( s == IntPtr.Zero )
					return 0;
				int fieldLen = Util.StrLen(s);
				if( !_connection._oldBinaryFormat )
				{
					// decode 
					tempBuffer = Util.AllocateUnmanagedMemory(fieldLen+1);
					fieldLen = sqlite_decode_binary(s,tempBuffer);
					s = tempBuffer;
				}
				if( buffer == null )
					length = fieldLen;
				else
				{
					IntPtr src = (IntPtr)(s.ToInt32() + fieldOffset);
					if ((fieldOffset + length) > fieldLen)
						length = fieldLen - (int) fieldOffset;
					if ((bufferoffset + length) > buffer.Length)
						length = buffer.Length - bufferoffset;
					Marshal.Copy (src, buffer, bufferoffset, length);
				}
				if( tempBuffer != IntPtr.Zero )
					Util.FreeUnmanagedMemory(tempBuffer);
				return length;
			}

			private IntPtr GetColumnPtr(int iCol)
			{
				return (IntPtr)Marshal.ReadInt32(Value,iCol*IntPtr.Size);
			}

			#endregion

			#region Implementation-specific private functions
			private int bind_old_binary( int idx, byte[] bytes )
			{
				if( Array.IndexOf(bytes,(byte)0,0,bytes.Length) >= 0 )
					throw new BinaryDataWithZerosException();
				unsafe
				{
					fixed( void *b = bytes )
					{
						return sqlite_bind(Handle,idx,new IntPtr(b),bytes.Length,1);
					}
				}
			}

			private int bind_new_binary( int idx, byte[] bytes )
			{
				unsafe
				{
					fixed( void *b = bytes )
					{
						IntPtr inPtr = new IntPtr(b);
						int size = sqlite_encode_binary(inPtr,bytes.Length,IntPtr.Zero);
						IntPtr outPtr = Util.AllocateUnmanagedMemory(size+1);
						sqlite_encode_binary(inPtr,bytes.Length,outPtr);
						int res = sqlite_bind(Handle,idx,outPtr,-1,1);
						Util.FreeUnmanagedMemory(outPtr);
						return res;
					}
				}
			}
			#endregion
		}

		#endregion
	}
}
