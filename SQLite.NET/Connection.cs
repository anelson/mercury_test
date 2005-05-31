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
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER "AS IS" AND ANY EXPRESS
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
using System.Collections;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Finisar.SQLite
{
	enum DateTimeFormat 
	{
		ISO8601,
		Ticks,
		CurrentCulture
	}

	sealed public class SQLiteConnection : IDbConnection, IDisposable
	{
		internal isqlite sqlite = null;
		internal ConnectionState mState = ConnectionState.Closed;
		internal SQLiteTransaction mpTrans;
		internal ArrayList mpCmds = new ArrayList();

		private String mpConnStr = "";
		private int mDataReaderCount = 0;

		public SQLiteConnection () {}

		public SQLiteConnection (String pConnStr)
		{
			ConnectionString = pConnStr;
		}

		public void Dispose ()
		{
			Close();
		}

		internal void AttachCommand (SQLiteCommand pCmd)
		{
			mpCmds.Add (pCmd);
		}

		internal void DetachCommand (SQLiteCommand pCmd)
		{
			pCmd.UnCompile();
			mpCmds.Remove (pCmd);
		}

		internal void AttachDataReader(SQLiteDataReader pReader)
		{
			++mDataReaderCount;
			mState = ConnectionState.Executing;
		}

		internal void DetachDataReader(SQLiteDataReader pReader)
		{
			--mDataReaderCount;
			if( mDataReaderCount == 0 && mState == ConnectionState.Executing )
				mState = ConnectionState.Open;
			if( (pReader.mCmdBehavior & CommandBehavior.CloseConnection) != 0 )
			{
				if( mDataReaderCount == 0 )
					Close();
				else
					throw new InvalidOperationException();
			}
		}

		public String ConnectionString
		{
			get
			{
				return mpConnStr;
			}
			set
			{
				if (value == null)
					throw new ArgumentNullException();
				if (mState != ConnectionState.Closed)
					throw new InvalidOperationException();
				mpConnStr = value;
			}
		}

		public int ConnectionTimeout
		{
			get
			{
				return 0;
			}
		}

		public String Database
		{
			get
			{
				return "";
			}
		}

		public ConnectionState State
		{
			get
			{
				return mState;
			}
		}

		IDbTransaction IDbConnection.BeginTransaction ()
		{
			return BeginTransaction();
		}

		IDbTransaction IDbConnection.BeginTransaction(IsolationLevel level)
		{
			return BeginTransaction(level);
		}

		public SQLiteTransaction BeginTransaction()
		{
			if (mState != ConnectionState.Open)
			{
				throw new InvalidOperationException();
			}
			if (mpTrans != null)
			{
				throw new NotSupportedException();
			}
			return mpTrans = new SQLiteTransaction(this);
		}

		public SQLiteTransaction BeginTransaction(IsolationLevel level)
		{
			return BeginTransaction();
		}

		public void ChangeDatabase (String pNewDB)
		{
			throw new NotSupportedException();
		}

		public void Open ()
		{
			if (mState != ConnectionState.Closed)
				throw new InvalidOperationException();

			// Parameter help
			String pHelp =
				"Valid parameters:\n" +
				"Data Source=<database file>  (required)\n" +
				"Version=<version of SQLite (2 or 3)>  (default: 2)\n" +
				"New=True|False  (default: False)\n" +
				"Compress=True|False  (default: False)\n" +
				"UTF8Encoding=True|False  (default: False)\n" +
				"UTF16Encoding=True|False  (default: False)\n" +
				//S"Temp Store=File|Memory  (default: File)\n" + 
				"Cache Size=<N>  (default: 2000)\n" +
				"Synchronous=Full|Normal|Off  (default: Normal)\n" +
				"DateTimeFormat=ISO8601|Ticks|CurrentCulture  (default: ISO8601)\n" +
				"Compatibility=[old-date-format][,old-binary-format] (default: None)";

			// Parameter defaults
			String pDB = null;
			bool newFile = false;
			bool compress = false;
			String pSync = "Normal";
			//String pTempStore = "File";
			String pCacheSize = "2000";
			bool UTF8Encoding = false;
			bool version3 = false;
			bool UTF16Encoding = false;
			DateTimeFormat DTFormat = DateTimeFormat.ISO8601;
			bool oldDateTimeFormat = false;
			bool oldBinaryFormat = false;

			// Parse connection string
			String [] parameters = mpConnStr.Split (";".ToCharArray());
			if (parameters.Length == 0)
				throw new SQLiteException (pHelp);
			for (int i = 0; i < parameters.Length; i++)
			{
				String p = parameters[i].Trim();
				if( p.Length == 0 )	// ignore empty item
					continue;

				int index = p.IndexOf('=');
				if( index < 0 )
					throw new SQLiteException (pHelp);

				String paramName = p.Substring(0,index).Trim();
				String paramValue = p.Substring(index+1).Trim();

				if (paramName.Equals ("Data Source"))
					pDB = paramValue;

				else if (paramName.Equals ("Version"))
					version3 = paramValue.Equals ("3");

				else if (paramName.Equals ("New"))
					newFile = paramValue.Equals ("True");

				else if (paramName.Equals ("Compress"))
					compress = paramValue.Equals ("True");

				else if (paramName.Equals ("UTF8Encoding"))
					UTF8Encoding = paramValue.Equals ("True");

				else if (paramName.Equals ("UTF16Encoding"))
					UTF16Encoding = paramValue.Equals ("True");

				else if (paramName.Equals ("Synchronous"))
					pSync = paramValue;

				else if (paramName.Equals ("Cache Size"))
					pCacheSize = paramValue;

					//		else if (paramName.Equals ("Temp Store"))
					//			pTempStore = paramValue;

				else if (paramName.Equals ("DateTimeFormat")) 
				{
					if( string.Compare(paramValue,"ISO8601",true) == 0 )
						DTFormat = DateTimeFormat.ISO8601;
					else if( string.Compare(paramValue,"Ticks",true) == 0 )
						DTFormat = DateTimeFormat.Ticks;
					else if( string.Compare(paramValue,"CurrentCulture",true) == 0 )
						DTFormat = DateTimeFormat.CurrentCulture;
				}

				else if (paramName.Equals ("Compatibility"))
				{
					string compatibilityValues = paramValue;
					foreach( string s in compatibilityValues.Split(',') )
					{
						string val = s.Trim();
						if( string.Compare(val,"old-date-format",true) == 0 )
							oldDateTimeFormat = true;
						else if( string.Compare(val,"old-binary-format",true) == 0 )
							oldBinaryFormat = true;
					}
				}
				else
					throw new SQLiteException (pHelp);
			}
			
			if (pDB == null)
				throw new SQLiteException (pHelp);

			if (File.Exists (pDB))
			{
				if (newFile)
				{
					// Try to delete existing file
					try
					{
						File.Delete (pDB);
						File.Delete (String.Concat (pDB, "-journal"));
					}
					catch (IOException)
					{
						throw new SQLiteException ("Cannot create new file, the existing file is in use.");
					}
					catch (Exception)
					{
					}
				}
			}
			else
			{
				if (!newFile)
				{
					try
					{
						pDB = System.IO.Path.GetFullPath(pDB);
					}
					catch(Exception )
					{
					}
					throw new SQLiteException (String.Concat("File '",pDB,"' does not exist. Use ConnectionString parameter New=True to create new file."));
				}
			}

			// Open connection
			if( compress )
				Util.CompressFile(pDB);

			// instantiate sqlite 
			if( version3 )
				sqlite = new sqlite3(UTF16Encoding,DTFormat,oldDateTimeFormat);
			else
				sqlite = new sqlite2(UTF8Encoding,DTFormat,oldDateTimeFormat,oldBinaryFormat);

			sqlite.open(pDB);

			mState = ConnectionState.Open;

			// Do Pragma's
			String pPragma;

			pPragma = String.Concat ("pragma synchronous = ", pSync);
			sqlite.exec (pPragma);

			pPragma = String.Concat ("pragma cache_size = ", pCacheSize);
			sqlite.exec (pPragma);

			// TODO temp_store pragma not working (?!?)
			//		pPragma = String.Concat ("pragma temp_store = ", pTempStore);
			//		sqlite.exec (pPragma);
		}

		public void Close ()
		{
			if (mState != ConnectionState.Closed)
			{
				if (mpTrans != null)
					mpTrans.Rollback();
				Debug.Assert (mpTrans == null);
				for (int i = 0; i < mpCmds.Count; ++i)
					((SQLiteCommand)mpCmds[i]).UnCompile();
				mState = ConnectionState.Closed;
				if( sqlite != null )
				{
					sqlite.close();
					sqlite = null;
				}
			}
		}

		public void Vacuum ()
		{
			if (mState != ConnectionState.Open)
				throw new InvalidOperationException();
			sqlite.exec("vacuum");
		}

		IDbCommand IDbConnection.CreateCommand ()
		{
			return new SQLiteCommand ("", this);
		}

		public SQLiteCommand CreateCommand ()
		{
			return new SQLiteCommand ("", this);
		}

		public long GetLastInsertRowId() {
			if (mState != ConnectionState.Open)
				throw new InvalidOperationException();

			//auto-increment columns are only supported in version 3
			if (! (sqlite is sqlite3)) {
				throw new NotSupportedException();
			}
			return ((sqlite3)sqlite).get_last_insert_rowid();
		}
	}

} // namespace Finisar
