//////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2003-2004, Finisar Corporation
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice,
//   this list of conditions and the following disclaimer. 
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// * Neither the name of the Finisar Corporation nor the names of its
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
using System.Collections.Specialized;
using System.Text;
using System.Text.RegularExpressions;
using System.Globalization;

namespace Finisar.SQLite
{
	sealed public class SQLiteCommand : IDbCommand
	{
		#region Members
		private String mpCmdText = "";
		private UpdateRowSource mUpdatedRowSource = UpdateRowSource.Both;
		private SQLiteParameterCollection mpParams = new SQLiteParameterCollection();
		private ArrayList mStatements = new ArrayList();
		internal int mTimeout = 30;
		internal SQLiteConnection mpConn;
		private bool mServingDataReader = false;
		#endregion

		#region Constructors
		public SQLiteCommand () {}

		public SQLiteCommand (String pCmdText)
		{
			CommandText = pCmdText;
		}

		public SQLiteCommand (String pCmdText, SQLiteConnection pConn)
		{
			if (pConn == null)
				throw new ArgumentNullException();
			CommandText = pCmdText;
			Connection = pConn;
		}
		#endregion

		#region IDisposable members
		public void Dispose ()
		{
			mpParams.Dispose();
			// DetachCommand() will call UnCompile()
			if (mpConn != null)
				mpConn.DetachCommand (this);
		}
		#endregion

		#region IDbCommand members
		public String CommandText
		{
			get
			{
				return mpCmdText;
			}
			set
			{
				if (mpConn != null && mServingDataReader)
					throw new InvalidOperationException("Can not set CommandText when the connection is busy serving data readers");
				UnCompile();
				if (value == null)
					throw new ArgumentNullException();

				mpCmdText = value;

				ParseCommand(mpCmdText,mStatements);
			}
		}

		public Int32 CommandTimeout
		{
			get
			{
				return mTimeout;
			}
			set
			{
				if (value < 0)
					throw new ArgumentException();
				mTimeout = value;
			}
		}

		public CommandType CommandType
		{
			get
			{
				return CommandType.Text;
			}
			set
			{
				if (value != CommandType.Text)
					throw new ArgumentOutOfRangeException();
			}
		}

		IDbConnection IDbCommand.Connection
		{
			get
			{
				return Connection;
			}
			set
			{
				Connection = (SQLiteConnection)value;
			}
		}

		public SQLiteConnection Connection
		{
			get
			{
				return mpConn;
			}
			set
			{
				if (mpConn != null)
				{
					if (mpConn.mState == ConnectionState.Executing)
						throw new InvalidOperationException();
					// DetachCommand() will call UnCompile()
					mpConn.DetachCommand (this);
				}
				if (value == null)
					mpConn = null;
				else
				{
					mpConn = value;
					mpConn.AttachCommand (this);
				}
			}
		}

		IDataParameterCollection IDbCommand.Parameters
		{
			get
			{
				return Parameters;
			}
		}

		public SQLiteParameterCollection Parameters
		{
			get
			{
				return mpParams;
			}
		}

		IDbTransaction IDbCommand.Transaction
		{
			get
			{
				return Transaction;
			}
			set
			{
				Transaction = (SQLiteTransaction)value;
			}
		}

		public SQLiteTransaction Transaction
		{
			get
			{
				if (mpConn != null)
					return mpConn.mpTrans;
				return null;
			}
			set
			{
				if( value != null )
					Connection = value.Connection;
			}
		}

		public UpdateRowSource UpdatedRowSource
		{
			get
			{
				return mUpdatedRowSource;
			}
			set
			{
				if (mpConn != null && mpConn.mState == ConnectionState.Executing)
					throw new InvalidOperationException();
				mUpdatedRowSource = value;
			}
		}

		public void Cancel ()
		{
			throw new NotSupportedException();
		}

		IDbDataParameter IDbCommand.CreateParameter()
		{
			return CreateParameter();
		}

		public SQLiteParameter CreateParameter()
		{
			return new SQLiteParameter(null,DbType.AnsiString);
		}

		public int ExecuteNonQuery ()
		{
			int result = 0;
			for( int i=0 ; i < GetSQLStatementCount() ; ++i )
			{
				OneSQLStatement s = GetSQLStatement(i);
				s.Compile();

				mpConn.sqlite.busy_timeout (mTimeout);
				int res = s.GetVM().step();
				if (res == SQLITE.DONE || res == SQLITE.ROW)
				{
					s.GetVM().reset();
					result += mpConn.sqlite.last_statement_changes ();
				}
				else if (res == SQLITE.ERROR)
					s.UnCompile(); // UnCompile will throw exception with appropriate msg.
				else
					throw new SQLiteException ("Unknown SQLite error");
			}
			return result;
		}

		IDataReader IDbCommand.ExecuteReader()
		{
			return ExecuteReader();
		}

		IDataReader IDbCommand.ExecuteReader(CommandBehavior cmdBehavior)
		{
			return ExecuteReader(cmdBehavior);
		}

		public SQLiteDataReader ExecuteReader()
		{
			return ExecuteReader(CommandBehavior.Default);
		}

		public SQLiteDataReader ExecuteReader(CommandBehavior cmdBehavior)
		{
			if (mpConn == null || !(mpConn.mState == ConnectionState.Open || mpConn.mState == ConnectionState.Executing))
				throw new InvalidOperationException("The connection must be open to call ExecuteReader");
			return new SQLiteDataReader(this,cmdBehavior);
		}

		public Object ExecuteScalar ()
		{
			// ExecuteReader and get value of first column of first row
			IDataReader pReader = ExecuteReader();
			Object pObj = null;
			if (pReader.Read() && pReader.FieldCount > 0)
				pObj = pReader.GetValue (0);
			pReader.Close();
			return pObj;
		}

		public void Prepare ()
		{
			// no-op
		}
		#endregion

		#region SQLite specific functions
		public void CreateAndAddUnnamedParameters()
		{
			// Parameters are unnamed
			int count = 0;
			for( int i=0 ; i < GetSQLStatementCount() ; ++i )
			{
				OneSQLStatement s = GetSQLStatement(i);
				count += s.GetUnnamedParameterCount();
			}

			while( count-- > 0 )
				this.Parameters.Add( CreateParameter() );
		}
		#endregion

		#region Parsing functions
		private const char Quote = '\'';
		private const char DoubleQuote = '"';
		private const char StatementTerminator = ';';
		private const char NamedParameterBeginChar = '@';
		private const char UnnamedParameterBeginChar = '?';
		private const char CreateTriggerBeginChar1 = 'C';
		private const char CreateTriggerBeginChar2 = 'c';

		private static Regex CreateTriggerRegExp = new Regex(@"(?i)CREATE\s+(?:TEMP\w*\s+)*TRIGGER");

		private static void AppendUpToFoundIndex( StringBuilder sb, string cmd, int foundIndex, ref int index )
		{
			if( foundIndex < 0 )
				foundIndex = cmd.Length;
			sb.Append( cmd, index, foundIndex-index );
			index = foundIndex;
		}

		private static void AppendUpToSameChar( StringBuilder sb, string cmd, int foundIndex, out int nextIndex )
		{
			int i = cmd.IndexOf(cmd[foundIndex],foundIndex+1);
			if( i < 0 )
			{
				sb.Append(cmd,foundIndex,cmd.Length-foundIndex);
				nextIndex = cmd.Length;
			}
			else
			{
				sb.Append(cmd,foundIndex,i-foundIndex+1);
				nextIndex = i+1;
			}
		}

		private static void AppendNamedParameter( StringBuilder sb, string cmd, int foundIndex, ArrayList paramNames, out int index )
		{
			index = foundIndex+1; 
			do
			{
				char ch = cmd[index];
				if( !(Char.IsLetterOrDigit(ch) || ch == '_') )
					break;
				++index;
			}
			while( index < cmd.Length );
			sb.Append('?');
			paramNames.Add( cmd.Substring(foundIndex,index-foundIndex) );
		}

		private void TerminateStatement( StringBuilder sb, ref ArrayList paramNames, ArrayList statements )
		{
			// One SQL statement is terminated.
			// Add this statement to the collection and
			// reset all string builders
			string cmd = sb.ToString().Trim();
			if( cmd.Length > 0 )
				statements.Add( new OneSQLStatement(this,cmd,paramNames) );
			sb.Length = 0;
			paramNames = new ArrayList();
		}

		private static void AppendTrigger( StringBuilder sb, string cmd, int foundIndex, out int index )
		{
			if( !CreateTriggerRegExp.IsMatch(cmd,foundIndex) )
			{
				sb.Append(cmd[foundIndex]);
				index = foundIndex+1;
				return;
			}

			const string END = "END";

			// search the keyword END
			char[] Terminators = 
				{
					Quote,
					DoubleQuote,
					'e',
					'E'
				};

			index = foundIndex;
			while( index < cmd.Length )
			{
				foundIndex = cmd.IndexOfAny(Terminators,index);
				AppendUpToFoundIndex(sb,cmd,foundIndex,ref index);
				if( foundIndex < 0 )
					break;

				switch( cmd[foundIndex] )
				{
					case Quote:
					case DoubleQuote:
						AppendUpToSameChar(sb,cmd,foundIndex,out index);
						break;

					case 'e':
					case 'E':
						if( string.Compare(cmd,foundIndex,END,0,END.Length,true,CultureInfo.InvariantCulture) == 0 )
						{
							sb.Append(cmd,foundIndex,END.Length);
							index = foundIndex+END.Length+1;
							return;
						}
						else
						{
							sb.Append(cmd[foundIndex]);
							index = foundIndex+1;
						}
						break;
				}
			}
		}

		/// <summary>
		/// parse the command text and 
		/// split it into separate SQL commands
		/// </summary>
 		private void ParseCommand( string cmd, ArrayList statements )
		{
			statements.Clear();

			if( cmd.Length == 0 )
				return;

			char[] Terminators = 
				{	StatementTerminator,
					NamedParameterBeginChar,
					Quote,
					DoubleQuote,
					UnnamedParameterBeginChar,
					CreateTriggerBeginChar1,
					CreateTriggerBeginChar2
				};

			int index = 0;
			ArrayList paramNames = new ArrayList();
			StringBuilder sb = new StringBuilder(cmd.Length);
			while( index < cmd.Length )
			{
				int foundIndex = cmd.IndexOfAny(Terminators,index);
				AppendUpToFoundIndex(sb,cmd,foundIndex,ref index);
				if( foundIndex < 0 )
					break;

				switch( cmd[foundIndex] )
				{
					case Quote:
					case DoubleQuote:
						AppendUpToSameChar(sb,cmd,foundIndex,out index);
						break;

					case NamedParameterBeginChar:
						AppendNamedParameter(sb,cmd,foundIndex,paramNames,out index);
						break;

					case UnnamedParameterBeginChar:
						paramNames.Add(null);
						sb.Append(UnnamedParameterBeginChar);
						index = foundIndex+1;
						break;

					case StatementTerminator:
						TerminateStatement(sb,ref paramNames,statements);
						index = foundIndex+1;
						break;

					case CreateTriggerBeginChar1:
					case CreateTriggerBeginChar2:
						AppendTrigger(sb,cmd,foundIndex,out index);
						break;

					default:
						throw new SQLiteException("Found the unexpected terminator '"+cmd[foundIndex]+"' at the position "+foundIndex);
				}
			}
			TerminateStatement(sb,ref paramNames,statements);

			// now iterate all SQL statements and assign 
			// the starting index of unnamed parameters inside Parameters collection
			int unnamedParameterCount = 0;
			for( int i=0 ; i < statements.Count ; ++i )
			{
				OneSQLStatement s = (OneSQLStatement)statements[i];

				s.SetUnnamedParametersStartIndex(unnamedParameterCount);

				unnamedParameterCount += s.GetUnnamedParameterCount();
			}
		}
		#endregion

		#region Internal functions
		internal void AttachDataReader( SQLiteDataReader reader )
		{
			mServingDataReader = true;
			if( mpConn != null )
				mpConn.AttachDataReader(reader);
		}

		internal void DetachDataReader( SQLiteDataReader reader )
		{
			if( mpConn != null )
				mpConn.DetachDataReader(reader);
			UnCompile();
			mServingDataReader = false;
		}

		internal void UnCompile ()
		{
			for( int i=0 ; i < GetSQLStatementCount() ; ++i )
			{
				OneSQLStatement s = GetSQLStatement(i);
				s.UnCompile();
			}
		}

		internal SQLiteParameter FindUnnamedParameter( int index )
		{
			if( index >= 0 )
			{
				int count = mpParams.Count;
				for( int i=0 ; i < count ; ++i )
				{
					SQLiteParameter p = mpParams[i];
					if( p.ParameterName == null )
					{
						if( index-- == 0 )
							return p;
					}
				}
			}

			throw new SQLiteException( String.Concat("Can not find unnamed parameter with index=",index.ToString()) );
		}

		internal SQLiteParameter FindNamedParameter( String parameterName )
		{
			int index = mpParams.IndexOf(parameterName);
			if( index < 0 )
				throw new SQLiteException( String.Concat("Can not find the parameter with name '",parameterName,"'") );
			return mpParams[index];
		}

		internal OneSQLStatement GetSQLStatement( int index )
		{
			return (OneSQLStatement)mStatements[index];
		}

		internal int GetSQLStatementCount()
		{
			return mStatements.Count;
		}
		#endregion
	}

} // namespace Finisar
