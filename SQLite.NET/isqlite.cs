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

namespace Finisar.SQLite
{
	/*
	** Return values for sqlite_exec() and sqlite_step()
	*/
	internal class SQLITE
	{
		public const int OK        =  0 ; /* Successful result */
		public const int ERROR     =  1 ; /* SQL error or missing database */
		public const int INTERNAL  =  2 ; /* An internal logic error in SQLite */
		public const int PERM      =  3 ; /* Access permission denied */
		public const int ABORT     =  4 ; /* Callback routine requested an abort */
		public const int BUSY      =  5 ; /* The database file is locked */
		public const int LOCKED    =  6 ; /* A table in the database is locked */
		public const int NOMEM     =  7 ; /* A malloc() failed */
		public const int READONLY  =  8 ; /* Attempt to write a readonly database */
		public const int INTERRUPT =  9 ; /* Operation terminated by public const int interrupt() */
		public const int IOERR     = 10 ; /* Some kind of disk I/O error occurred */
		public const int CORRUPT   = 11 ; /* The database disk image is malformed */
		public const int NOTFOUND  = 12 ; /* (Internal Only) Table or record not found */
		public const int FULL      = 13 ; /* Insertion failed because database is full */
		public const int CANTOPEN  = 14 ; /* Unable to open the database file */
		public const int PROTOCOL  = 15 ; /* Database lock protocol error */
		public const int EMPTY     = 16 ; /* (Internal Only) Database table is empty */
		public const int SCHEMA    = 17 ; /* The database schema changed */
		public const int TOOBIG    = 18 ; /* Too much data for one row of a table */
		public const int CONSTRAINT= 19 ; /* Abort due to contraint violation */
		public const int MISMATCH  = 20 ; /* Data type mismatch */
		public const int MISUSE    = 21 ; /* Library used incorrectly */
		public const int NOLFS     = 22 ; /* Uses OS features not supported on host */
		public const int AUTH      = 23 ; /* Authorization denied */
		public const int FORMAT    = 24 ; /* Auxiliary database format error */
		public const int RANGE     = 25 ; /* 2nd parameter to sqlite_bind out of range */
		public const int NOTADB    = 26 ; /* File opened that is not a database file */
		public const int ROW       = 100; /* sqlite_step() has another row ready */
		public const int DONE      = 101; /* sqlite_step() has finished executing */
	}

	/// <summary>
	/// Represents the virtual machine to execute in 'compile/step/finalize' family
	/// </summary>
	internal interface isqlite_vm : IDisposable
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="pVm">The virtual machine to execute</param>
		int step();

		/// <summary>
		/// Delete the virtual machine
		/// </summary>
		//void Dispose();

		/// <summary>
		/// Reset the virtual machine.
		/// </summary>
		void reset();

		/// <summary>
		/// Replace the parameter of the index 'idx' with the value from 'p'
		/// </summary>
		/// <param name="idx"></param>
		/// <param name="p"></param>
		void bind(int idx, IDbDataParameter p);

		#region Data access functions
		int		GetColumnCount();
		string	GetColumnName(int iCol);

		string	GetColumnType(int iCol);

		bool	IsColumnNull(int iCol);
		bool	IsColumnEmptyString(int iCol);

		string	GetColumnValue(int iCol);
		int		GetColumnInt(int iCol);
		long	GetColumnLong(int iCol);
		double	GetColumnDouble(int iCol);
		decimal GetColumnDecimal(int iCol);
		DateTime GetColumnDateTime(int iCol);
		long	GetColumnBytes(int iCol, long fieldOffset, Byte [] buffer, int bufferoffset, int length );
		#endregion
	}

	internal interface isqlite
	{
		/// <summary>
		/// Open SQLite database. The native handle to the opened DB must
		/// be kept in the class implementing this interface.
		/// </summary>
		/// <param name="filename"></param>
		/// <returns></returns>
		void open(string filename);

		/// <summary>
		/// Close the database.
		/// </summary>
		void close();

		/// <summary>
		/// A function to executes one or more statements of SQL.
		/// </summary>
		/// <param name="sql">SQL to be executed</param>
		void exec( string sql );


		/// <summary>
		/// This function returns the number of database rows that were changed
		/// by the last INSERT, UPDATE, or DELETE statment
		/// </summary>
		/// <returns></returns>
		int last_statement_changes();

		/// <summary>
		/// This routine sets a busy handler that sleeps for a while when a
		/// table is locked.  The handler will sleep multiple times until 
		/// at least "ms" milleseconds of sleeping have been done.  After
		/// "ms" milleseconds of sleeping, the handler returns 0 which
		/// causes sqlite_exec() to return SQLITE_BUSY.
		/// Calling this routine with an argument less than or equal to zero
		/// turns off all busy handlers.
		/// </summary>
		/// <param name="ms"></param>
		void busy_timeout(int ms);

		/// <summary>
		/// Prepare the SQL statement for the subsequent passing to 'step' function.
		/// </summary>
		/// <param name="zSql">SQL statement to be compiled</param>
		/// <returns></returns>
		isqlite_vm compile( string zSql );
	}
}
