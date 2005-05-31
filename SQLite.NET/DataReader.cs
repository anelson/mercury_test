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
using System.Globalization;
using System.Data;
using System.Collections;
using System.Text;
using System.Runtime.InteropServices;

namespace Finisar.SQLite
{
	sealed public class SQLiteDataReader : IDataReader, IEnumerable
	{
		private SQLiteCommand mpCmd;
		private bool mDone = true;
		private bool mFirstRead = true;
		private String[] mFieldNames;
		private Type[] mFieldTypes;
		private int[] mColumnNameStartIndexes;
		private int mStatementIndex = 0;
		private int mRecordsAffected = 0;
		private OneSQLStatement mStmt;
		internal CommandBehavior mCmdBehavior;
		private bool mInitialized = false;

		internal SQLiteDataReader (SQLiteCommand pCmd, CommandBehavior cmdBehavior)
		{	
			if (pCmd == null)
				throw new ArgumentNullException();
			if( pCmd.GetSQLStatementCount() <= 0 )
				throw new ArgumentException("CommandText doesn't contain any SQL statements in SQLiteCommand");
			mpCmd = pCmd;
			mCmdBehavior = cmdBehavior;
		}

		private void EnsureInitialization()
		{
			if( !mInitialized )
			{
				mInitialized = true;
				mpCmd.AttachDataReader(this);
				ExecuteFirstStep();
			}
		}

		private void ExecuteFirstStep()
		{
			mDone = true;
			mFirstRead = true;

			// Exec first step and get column info.
			mStmt = mpCmd.GetSQLStatement(mStatementIndex); 
			mStmt.Compile();
			mpCmd.mpConn.sqlite.busy_timeout (mpCmd.mTimeout);
			isqlite_vm vm = mStmt.GetVM();
			int res = vm.step();
			if (res == SQLITE.ROW || res == SQLITE.DONE)
			{
				mFieldNames = new String[vm.GetColumnCount()];
				mFieldTypes = new Type[vm.GetColumnCount()];
				mColumnNameStartIndexes = new int[vm.GetColumnCount()];
				// Get column info.
				for (int i = 0; i < vm.GetColumnCount(); ++i)
				{
					mFieldNames[i] = vm.GetColumnName(i);
					mFieldTypes[i] = SQLType2Type ( vm.GetColumnType(i) );
					mColumnNameStartIndexes[i] = mFieldNames[i].IndexOf('.')+1;
				}
				mDone = res == SQLITE.DONE;
			}
			else if (res == SQLITE.ERROR)
				mStmt.UnCompile(); // UnCompile will throw exception with appropriate msg.
			else
				throw new SQLiteException ("Unknown SQLite error");
		}

		private static Type SQLType2Type (String pTypeStr)
		{
			if( pTypeStr != null )
				pTypeStr = pTypeStr.ToUpper(CultureInfo.InvariantCulture);
			if (
				pTypeStr == null
				|| pTypeStr.IndexOf ("CHAR") != -1
				|| pTypeStr.IndexOf ("TEXT") != -1)
				return typeof (String);
			else if (
				pTypeStr.IndexOf ("SMALLINT") != -1)
				return typeof (Int16);
			else if (
				pTypeStr.IndexOf ("BIGINT") != -1)
				return typeof (Int64);
			else if (
				pTypeStr.IndexOf ("INT") != -1) 
			{
				// TODO: We need to look at the actual data to determine the minimal type.
				return typeof (Int64);
			}
			else if (
				pTypeStr.IndexOf ("DOUBLE") != -1
				|| pTypeStr.IndexOf ("REAL") != -1)
				return typeof (Double);
			else if (
				pTypeStr.IndexOf ("FLOAT") != -1)
				return typeof (Single);
			else if (
				pTypeStr.IndexOf ("BIT") != -1
				|| pTypeStr.IndexOf ("BOOL") != -1)
				return typeof (Boolean);
			else if (
				pTypeStr.IndexOf ("NUMERIC") != -1
				|| pTypeStr.IndexOf ("DECIMAL") != -1
				|| pTypeStr.IndexOf ("MONEY") != -1)
				return typeof (Decimal);
			else if (
				pTypeStr.IndexOf ("DATE") != -1
				|| pTypeStr.IndexOf ("TIME") != -1)
				return typeof (DateTime);
			else if (
				pTypeStr.IndexOf ("BLOB") != -1
				|| pTypeStr.IndexOf ("BINARY") != -1)
				return typeof (Byte[]);
			return typeof (String);
		}

		public void Dispose ()
		{
			Close();
		}

		public int Depth
		{
			get
			{
				// ?
				return 0;
			}
		}

		public bool IsClosed
		{
			get
			{
				return mpCmd == null;
			}
		}

		public int RecordsAffected
		{
			get
			{
				return mRecordsAffected;
			}
		}

		public void Close ()
		{
			if (mpCmd != null)
			{
				CalculateRecordsAffected();
				mpCmd.DetachDataReader(this);
				mpCmd = null;
				mFieldNames = null;
				mFieldTypes = null;
			}
		}

		private void CalculateRecordsAffected()
		{
			EnsureInitialization();
			if( mStmt != null )
			{
				isqlite_vm vm = mStmt.GetVM();
				if( vm != null )
					vm.reset();
			}
			if( mpCmd != null && mpCmd.mpConn != null && mpCmd.mpConn.sqlite != null )
				mRecordsAffected += mpCmd.mpConn.sqlite.last_statement_changes();
		}

		public DataTable GetSchemaTable ()
		{
			EnsureInitialization();
			DataTable pSchemaTable = new DataTable ("Schema");

			pSchemaTable.Columns.Add ("ColumnName", typeof (String));
			pSchemaTable.Columns.Add ("ColumnOrdinal", typeof (Int32));
			pSchemaTable.Columns.Add ("ColumnSize", typeof (Int32));
			pSchemaTable.Columns.Add ("NumericPrecision", typeof (Int32));
			pSchemaTable.Columns.Add ("NumericScale", typeof (Int32));
			pSchemaTable.Columns.Add ("DataType", typeof (Type));
			pSchemaTable.Columns.Add ("ProviderType", typeof (Int32));
			pSchemaTable.Columns.Add ("IsLong", typeof (Boolean));
			pSchemaTable.Columns.Add ("AllowDBNull", typeof (Boolean));
			pSchemaTable.Columns.Add ("IsReadOnly", typeof (Boolean));
			pSchemaTable.Columns.Add ("IsRowVersion", typeof (Boolean));
			pSchemaTable.Columns.Add ("IsUnique", typeof (Boolean));
			pSchemaTable.Columns.Add ("IsKey", typeof (Boolean));
			pSchemaTable.Columns.Add ("IsAutoIncrement", typeof (Boolean));
			pSchemaTable.Columns.Add ("BaseSchemaName", typeof (String));
			pSchemaTable.Columns.Add ("BaseCatalogName", typeof (String));
			pSchemaTable.Columns.Add ("BaseTableName", typeof (String));
			pSchemaTable.Columns.Add ("BaseColumnName", typeof (String));

			pSchemaTable.BeginLoadData();
			for (Int32 i = 0; i < mFieldNames.Length; ++i)
			{
				DataRow pSchemaRow = pSchemaTable.NewRow();

				// This may be as good as we can do.
				string columnName = GetName(i);
				pSchemaRow["ColumnName"] = columnName;
				pSchemaRow["ColumnOrdinal"] = i;
				pSchemaRow["ColumnSize"] = 0;
				pSchemaRow["NumericPrecision"] = 0;
				pSchemaRow["NumericScale"] = 0;
				pSchemaRow["DataType"] = mFieldTypes[i];
				pSchemaRow["ProviderType"] = GetProviderType(mFieldTypes[i]);
				pSchemaRow["IsLong"] = (false);
				pSchemaRow["AllowDBNull"] = (true);
				pSchemaRow["IsReadOnly"] = (false);
				pSchemaRow["IsRowVersion"] = (false);
				pSchemaRow["IsUnique"] = (false);
				pSchemaRow["IsKey"] = (false);
				pSchemaRow["IsAutoIncrement"] = (false);
				pSchemaRow["BaseSchemaName"] = "";
				pSchemaRow["BaseCatalogName"] = "";
				pSchemaRow["BaseTableName"] = "";
				pSchemaRow["BaseColumnName"] = columnName;

				pSchemaTable.Rows.Add (pSchemaRow);
			}
			// Enhance schema support for SELECT command.
			// It's necessary to build SQLiteCommandBuilder
			EnhanceSchemaTable(pSchemaTable);
			pSchemaTable.EndLoadData();

			return pSchemaTable;
		}

		public bool NextResult ()
		{
			if (mpCmd == null)
				throw new InvalidOperationException();

			EnsureInitialization();
			++mStatementIndex;
			bool success = mStatementIndex < mpCmd.GetSQLStatementCount();
			if( success )
			{
				CalculateRecordsAffected();
				ExecuteFirstStep();
			}
			return success;
		}

		public bool Read ()
		{
			if (mpCmd == null)
				throw new InvalidOperationException();

			EnsureInitialization();
			if (mDone)
				return false;
			if (mFirstRead)
			{
				mFirstRead = false;
				return true;
			}
			int res = mStmt.GetVM().step();
			if (res == SQLITE.ROW)
				return true;
			else if (res == SQLITE.DONE)
				mDone = true;
			else if (res == SQLITE.ERROR)
				mStmt.UnCompile(); // UnCompile will throw exception with appropriate msg.
			else
				throw new SQLiteException ("Unknown SQLite error");
			return false;
		}

		public int FieldCount
		{
			get
			{
				EnsureInitialization();
				return mFieldNames.Length;
			}
		}

		public Object this[String name]
		{
			get
			{
				return this[GetOrdinal (name)];
			}
		}

		public Object this[int i]
		{
			get
			{
				return GetValue(i);
			}
		}

		public bool GetBoolean (int i)
		{
			int val = 0;
			try { val = GetInt32(i); }
			catch (FormatException) {}

			// if val is zero, it could be the format exception or the invalid data. 
			if( val != 0 )
				return true;

			String s = GetString(i);
			try { return Boolean.Parse(s); }
			catch (FormatException) {}

			s = s.TrimStart();

			// now find the first non-digit and parse the string up to this character
			const int MaxLen = 21;
			int lengthToSearch = s.Length > MaxLen ? MaxLen : s.Length;
			int len = 0;
			while( len < lengthToSearch )
			{
				char c = s[len];
				if( c < '0' || c > '9' )
				{
					if( len > 0 || (c != '-' && c != '+') )
						break;
				}
				++len;
			}

			try { return Int32.Parse(s.Substring(0,len)) != 0; }
			catch( FormatException ) {}

			return false;
		}

		public Byte GetByte (int i)
		{
			return (Byte)GetInt32(i);
		}

		// todo - use binary decoder function
		public long GetBytes (
			int i,
			long fieldOffset,
			Byte [] buffer,
			int bufferoffset,
			int length
			)
		{
			return mStmt.GetVM().GetColumnBytes(i,fieldOffset,buffer,bufferoffset,length);
		}

		public Char GetChar (int i)
		{
			string s = GetString(i);
			return s[0];	// will throw IndexOufRangeException if the length of the string is zero
		}

		public long GetChars (
			int i,
			long fieldoffset,
			Char[] buffer,
			int bufferoffset,
			int length
			)
		{
			String pStr = GetString(i);
			if (buffer == null)
				return pStr.Length;
			if ((fieldoffset + length) > pStr.Length)
				length = pStr.Length - (int) fieldoffset;
			if ((bufferoffset + length) > buffer.Length)
				length = buffer.Length - bufferoffset;
			pStr.CopyTo ((int) fieldoffset, buffer, bufferoffset, length);
			return length;
		}

		public IDataReader GetData (int i)
		{
			throw new NotSupportedException();
		}

		public String GetDataTypeName (int i)
		{
			EnsureInitialization();
			return mFieldTypes[i].Name;
		}

		public DateTime GetDateTime (int i)
		{
			return mStmt.GetVM().GetColumnDateTime(i);
		}

		public Decimal GetDecimal (int i)
		{
			return mStmt.GetVM().GetColumnDecimal(i);
		}

		public double GetDouble (int i)
		{
			return mStmt.GetVM().GetColumnDouble(i);
		}

		public Type GetFieldType (int i)
		{
			EnsureInitialization();
			return mFieldTypes[i];
		}

		public float GetFloat (int i)
		{
			return (float)GetDouble(i);
		}

		public Guid GetGuid (int i)
		{
			return new Guid(GetString(i));
		}

		public short GetInt16 (int i)
		{
			return (short)GetInt32(i);
		}

		public int GetInt32 (int i)
		{
			return mStmt.GetVM().GetColumnInt(i);
		}

		public long GetInt64 (int i)
		{
			return mStmt.GetVM().GetColumnLong(i);
		}

		public String GetName (int i)
		{
			EnsureInitialization();
			return mFieldNames[i].Substring(mColumnNameStartIndexes[i]);
		}

		public int GetOrdinal (String name)
		{
			int i;
			for (i = 0; i < mFieldNames.Length; ++i)
				if (name.Equals (mFieldNames[i]))
					return i;
			for (i = 0; i < mFieldNames.Length; ++i)
				if (String.Compare (name, 0, mFieldNames[i], mColumnNameStartIndexes[i], System.Int32.MaxValue, true, System.Globalization.CultureInfo.InvariantCulture) == 0)
					return i;
			return -1;
		}

		public String GetString (int i)
		{
			string s = mStmt.GetVM().GetColumnValue(i);
			if( s == null )
				return "";
			return s;
		}

		public Object GetValue (int i)
		{
			if( !IsDBNull(i) )
			{
				Type type = mFieldTypes[i];
				if (type == typeof (String))
					return GetString (i);
				if( !mStmt.GetVM().IsColumnEmptyString(i) )
				{
					if (type == typeof (Double))
						return GetDouble(i);
					if (type == typeof (Int16))
						return GetInt16(i);
					if (type == typeof (Int32))
						return GetInt32(i);
					if (type == typeof (Int64))
						return GetInt64(i);
					if (type == typeof (Single))
						return GetFloat(i);
					if (type == typeof (Boolean))
						return GetBoolean(i);
					if (type == typeof (Decimal))
						return GetDecimal(i);
					if (type == typeof (DateTime))
						return GetDateTime(i);
					if (type == typeof (Byte[]))
					{
						int len = (int)GetBytes(i,0,null,0,0);
						if( len == 0 )
							return DBNull.Value;
						else
						{
							byte[] buffer = new byte[len];
							GetBytes(i,0,buffer,0,len);
							return buffer;
						}
					}
					throw new InvalidOperationException();
				}
			}
			return DBNull.Value;
		}

		public int GetValues (Object[] values)
		{
			int count = values.Length;
			if (mFieldNames.Length < count)
				count = mFieldNames.Length;
			for (int i = 0; i < count; ++i)
				values[i] = GetValue (i);
			return count;
		}

		public bool IsDBNull (int i)
		{
			return mStmt.GetVM().IsColumnNull(i);
		}

		public IEnumerator GetEnumerator()
		{
			return new DbEnumerator(this,true);
		}

		/// <summary>
		/// Try to assign correct values to the following columns:
		/// AllowDBNull
		/// IsUnique
		/// IsKey
		/// IsAutoIncrement
		/// BaseTableName
		/// </summary>
		/// <param name="schemaTable"></param>
		private void EnhanceSchemaTable( DataTable schemaTable )
		{
			const string SELECTText = "SELECT";

			OneSQLStatement stmt = mStmt;
			string commandText = stmt.CommandText.Trim();
			if( string.Compare(commandText,0,SELECTText,0,SELECTText.Length,true) != 0 )
				return;

			// The first step is to determine the table name.
			string baseTableName = FindBaseTableName(commandText);

			// BaseTableName was not found -> nothing to enhance
			if( baseTableName == null )
				return;

			// update BaseTableName column in the schema table
			foreach( DataRow row in schemaTable.Rows )
				row["BaseTableName"] = baseTableName;

			// the second step is to get the information about the table
			IDbCommand cmd = mpCmd.Connection.CreateCommand();
			cmd.CommandText = "PRAGMA table_info("+baseTableName+")";
			using( IDataReader tableInfoReader = cmd.ExecuteReader() )
			{
				while( tableInfoReader.Read() )
				{
					string columnName = Convert.ToString(tableInfoReader["name"]);
					DataRow row = FindRow(schemaTable,columnName);
					if( row != null )
					{
						string type = Convert.ToString(tableInfoReader["type"]);
						bool notNull = Convert.ToInt32(tableInfoReader["notnull"]) != 0;
						bool primaryKey = Convert.ToInt32(tableInfoReader["pk"]) != 0;

						row["AllowDBNull"] = !(primaryKey || notNull);
						row["IsUnique"] = primaryKey;
						row["IsKey"] = primaryKey;
						row["IsAutoIncrement"] = primaryKey && string.Compare(type,"INTEGER",true) == 0;
					}
				}
			}
		}

		// To do it, I search for the first 'FROM' token
		// not inside parenthesis and quotes, and extract
		// the word after 'FROM'.
		private static string FindBaseTableName( string commandText )
		{
			const string FROMText = "FROM";

			int parenthesisLevel = 0;
			bool insideQuote = false;

			for( int i=0 ; i < commandText.Length-FROMText.Length-1 ; i++ )
			{
				char c = commandText[i];
				if( c == '(' )
				{
					if( !insideQuote )
						++parenthesisLevel;	
				}
				else if( c == ')' )
				{
					if( !insideQuote )
					{
						if( --parenthesisLevel < 0 )
							throw new SQLiteException("Unmatched parenthesis at the position #"+i);
					}
				}
				else if( c == '\'' )
					insideQuote = !insideQuote;
				else if( c == 'F' || c == 'f' )
				{
					if( !insideQuote && 
						parenthesisLevel == 0 && 
						string.Compare(commandText.Substring(i,FROMText.Length),FROMText,true) == 0 )
					{
						string[] tokens = commandText.Substring(i+FROMText.Length).Trim().Split(null);
						if( tokens.Length > 0 )
							return tokens[0];
					}
				}
			}
			return null;
		}

		private static DataRow FindRow( DataTable table, string columnName )
		{
			foreach( DataRow row in table.Rows )
			{
				if( string.Compare(Convert.ToString(row["ColumnName"]),columnName,true) == 0 )
					return row;
			}
			return null;
		}

		private static DbType GetProviderType( Type type )
		{
			if (type == typeof (String))
				return DbType.String;
			if (type == typeof (Double))
				return DbType.Double;
			if (type == typeof (Int16))
				return DbType.Int16;
			if (type == typeof (Int32))
				return DbType.Int32;
			if (type == typeof (Int64))
				return DbType.Int64;
			if (type == typeof (Single))
				return DbType.Single;
			if (type == typeof (Boolean))
				return DbType.Boolean;
			if (type == typeof (Decimal))
				return DbType.Decimal;
			if (type == typeof (DateTime))
				return DbType.DateTime;
			if (type == typeof (Byte[]))
				return DbType.Binary;
			return DbType.String;
		}

		#region internal DbEnumerator
		/// <summary>
		/// Internal implementation of DbEnumerator.
		/// Unfortunately, Compact Framework doesn't support
		/// System.Data.Common.DbEnumerator even the documentation 
		/// says it's supported.
		/// </summary>
		class DbEnumerator : IEnumerator, IDisposable
		{
			private IDataReader _reader;
			private bool _closeReader;

			public DbEnumerator( IDataReader reader, bool closeReader )
			{
				_reader = reader;
				_closeReader = closeReader;
			}

			#region IEnumerator Members

			public void Reset()
			{
				throw new NotSupportedException();
			}

			public object Current
			{
				get
				{
					return _reader;
				}
			}

			public bool MoveNext()
			{
				bool success = _reader.Read();
				if( !success && _closeReader )
					_reader.Close();
				return success;
			}

			#endregion

			#region IDisposable Members

			public void Dispose()
			{
				if( _closeReader && _reader != null )
				{
					_reader.Dispose();
					_reader = null;
				}
			}

			#endregion
		}
		#endregion
	}
} // namespace Finisar
