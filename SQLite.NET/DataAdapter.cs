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
using System.Data.Common;
using System.Collections;
using System.Text;

namespace Finisar.SQLite
{
	public class SQLiteDataAdapter : DbDataAdapter, IDbDataAdapter
	{
		private SQLiteCommand mpDeleteCmd;
		private SQLiteCommand mpInsertCmd;
		private SQLiteCommand mpSelectCmd;
		private SQLiteCommand mpUpdateCmd;

		public event SQLiteRowUpdatingEventHandler RowUpdating;
		public event SQLiteRowUpdatedEventHandler RowUpdated;

		/// <summary>
		/// Initializes a new instance of the SQLiteDataAdapter class.
		/// </summary>
		public SQLiteDataAdapter() {}

		/// <summary>
		/// Initializes a new instance of the SQLiteDataAdapter class 
		/// with the specified SQLiteCommand as the SelectCommand property.
		/// </summary>
		/// <param name="selectCommand"></param>
		public SQLiteDataAdapter( SQLiteCommand selectCommand ) 
		{
			SelectCommand = selectCommand;
		}

		/// <summary>
		/// Initializes a new instance of the SQLiteDataAdapter class 
		/// with the specified SQLiteCommand as the SelectCommand property.
		/// </summary>
		/// <param name="selectCommand"></param>
		public SQLiteDataAdapter( IDbCommand selectCommand ) : this((SQLiteCommand)selectCommand) 
		{
		}

		/// <summary>
		/// Initializes a new instance of the SQLiteDataAdapter class 
		/// with a SelectCommand and a SQLiteConnection object.
		/// </summary>
		/// <param name="selectCommandText"></param>
		/// <param name="connection"></param>
		public SQLiteDataAdapter( string selectCommandText, SQLiteConnection connection )
		{
			SQLiteCommand cmd = connection.CreateCommand();
			cmd.CommandText = selectCommandText;
			SelectCommand = cmd;
		}

		/// <summary>
		/// Initializes a new instance of the SQLiteDataAdapter class 
		/// with a SelectCommand and a SQLiteConnection object.
		/// </summary>
		/// <param name="selectCommandText"></param>
		/// <param name="connection"></param>
		public SQLiteDataAdapter( string selectCommandText, IDbConnection connection ) 
			: this(selectCommandText,(SQLiteConnection)connection)
		{
		}

		/// <summary>
		/// Initializes a new instance of the SQLiteDataAdapter class 
		/// with a SelectCommand and a connection string.
		/// </summary>
		/// <param name="selectCommandText"></param>
		/// <param name="connectionString"></param>
		public SQLiteDataAdapter( string selectCommandText, string connectionString )
			: this(selectCommandText,new SQLiteConnection(connectionString))
		{
		}

		IDbCommand IDbDataAdapter.DeleteCommand
		{
			get
			{
				return DeleteCommand;
			}
			set
			{
				DeleteCommand = (SQLiteCommand)value;
			}
		}

		public SQLiteCommand DeleteCommand
		{
			get
			{
				return mpDeleteCmd;
			}
			set
			{
				mpDeleteCmd = value;
			}
		}

		IDbCommand IDbDataAdapter.InsertCommand
		{
			get
			{
				return InsertCommand;
			}
			set
			{
				InsertCommand = (SQLiteCommand)value;
			}
		}

		public SQLiteCommand InsertCommand
		{
			get
			{
				return mpInsertCmd;
			}
			set
			{
				mpInsertCmd = value;
			}
		}

		IDbCommand IDbDataAdapter.SelectCommand
		{
			get
			{
				return SelectCommand;
			}
			set
			{
				SelectCommand = (SQLiteCommand)value;
			}
		}

		public SQLiteCommand SelectCommand
		{
			get
			{
				return mpSelectCmd;
			}
			set
			{
				mpSelectCmd = value;
			}
		}

		IDbCommand IDbDataAdapter.UpdateCommand
		{
			get
			{
				return UpdateCommand;
			}
			set
			{
				UpdateCommand = (SQLiteCommand)value;
			}
		}

		public SQLiteCommand UpdateCommand
		{
			get
			{
				return mpUpdateCmd;
			}
			set
			{
				mpUpdateCmd = value;
			}
		}

		protected override RowUpdatedEventArgs CreateRowUpdatedEvent (
			DataRow dataRow,
			IDbCommand command,
			StatementType statementType,
			DataTableMapping tableMapping
			)
		{
			return new SQLiteRowUpdatedEventArgs (dataRow, command, statementType, tableMapping);
		}

		protected override RowUpdatingEventArgs CreateRowUpdatingEvent (
			DataRow dataRow,
			IDbCommand command,
			StatementType statementType,
			DataTableMapping tableMapping
			)
		{
			return new SQLiteRowUpdatingEventArgs (dataRow, command, statementType, tableMapping);
		}

		protected override void OnRowUpdating (RowUpdatingEventArgs args)
		{
			if( RowUpdating != null )
				RowUpdating(this,args);
		}

		protected override void OnRowUpdated (RowUpdatedEventArgs args)
		{
			if( RowUpdated != null )
				RowUpdated(this,args);
		}
	}

	sealed public class SQLiteRowUpdatingEventArgs : RowUpdatingEventArgs
	{
		public SQLiteRowUpdatingEventArgs (
			DataRow row,
			IDbCommand command,
			StatementType statementType,
			DataTableMapping tableMapping
			) : base (row, command, statementType, tableMapping) 
		{
		}

		public new SQLiteCommand Command
		{
			get { return (SQLiteCommand)base.Command; }
		}
	}

	sealed public class SQLiteRowUpdatedEventArgs : RowUpdatedEventArgs
	{
		public SQLiteRowUpdatedEventArgs (
			DataRow row,
			IDbCommand command,
			StatementType statementType,
			DataTableMapping tableMapping
			) : base(row, command, statementType, tableMapping) 
		{
		}

		public new SQLiteCommand Command
		{
			get { return (SQLiteCommand)base.Command; }
		}
	}

	public delegate void SQLiteRowUpdatingEventHandler( Object sender, RowUpdatingEventArgs e);
	public delegate void SQLiteRowUpdatedEventHandler( Object sender, RowUpdatedEventArgs e);

} // namespace Finisar
