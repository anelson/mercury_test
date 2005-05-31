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

using System;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Collections;

namespace Finisar.SQLite
{
	sealed public class SQLiteCommandBuilder
	{
		public	String				QuotePrefix;
		public  String				QuoteSuffix;

		private const string		OriginalVersionParameterPrefix = "@Original_";

		private SQLiteDataAdapter	mpAdapter;
		private DataTable			mpSchema;
		private String				mpTableName;

		private IDbCommand			mpUpdateCmd;
		private IDbCommand			mpInsertCmd;
		private IDbCommand			mpDeleteCmd;

		public SQLiteCommandBuilder()
		{
		}

		public SQLiteCommandBuilder( SQLiteDataAdapter pAdapter )
		{
			this.DataAdapter = pAdapter;
		}

		public SQLiteDataAdapter DataAdapter
		{
			get
			{
				return mpAdapter; 
			}
			set
			{
				if (mpAdapter != null) 
				{
					mpAdapter.RowUpdating -= new SQLiteRowUpdatingEventHandler(OnRowUpdating);
				}
				mpAdapter = value; 
				mpAdapter.RowUpdating += new SQLiteRowUpdatingEventHandler(OnRowUpdating);
			}
		}


		public void DeriveParameters(IDbCommand cmd)
		{
			throw new SQLiteException("DeriveParameters is not supported - SQLite does not support stored procedures");
		}

		public IDbCommand GetDeleteCommand()
		{
			if (mpDeleteCmd != null) return mpDeleteCmd;

			IDbCommand cmd = CreateBaseCommand();

			StringBuilder sb = new StringBuilder();
			sb.Append("DELETE FROM ");
			sb.Append(Quote(mpTableName));
			sb.Append(" WHERE ");
			sb.Append(CreateOriginalWhere(cmd));

			cmd.CommandText = sb.ToString();

			mpDeleteCmd = cmd;
			return cmd;
		}

		public IDbCommand GetInsertCommand()
		{
			if (mpInsertCmd != null) return mpInsertCmd;

			IDbCommand cmd = CreateBaseCommand();

			StringBuilder setstr = new StringBuilder();
			StringBuilder valstr = new StringBuilder();
			for(int i=0 ; i < mpSchema.Rows.Count; i++)
			{
				DataRow schemaRow = mpSchema.Rows[i];
				String colname = Quote(Convert.ToString(schemaRow["ColumnName"]));

				if (!IncludedInInsert(schemaRow)) continue;

				if (setstr.Length > 0) 
				{
					setstr.Append(", ");
					valstr.Append(", ");
				}

				IDbDataParameter p = CreateParameter(schemaRow, false);
				cmd.Parameters.Add(p);

				setstr.Append( colname );
				valstr.Append( p.ParameterName );
			}

			StringBuilder sb = new StringBuilder();

			sb.Append("INSERT INTO ");
			sb.Append(Quote(mpTableName));
			sb.Append(" (");
			sb.Append(setstr.ToString());
			sb.Append(") VALUES (");
			sb.Append(valstr.ToString());
			sb.Append(")");
			sb.Append("; ");
			sb.Append(CreateFinalSelect(true));

			cmd.CommandText = sb.ToString();

			mpInsertCmd = cmd;
			return cmd;
		}

		public IDbCommand GetUpdateCommand() 
		{
			if (mpUpdateCmd != null) return mpUpdateCmd; 

			IDbCommand cmd = CreateBaseCommand();

			StringBuilder setstr = new StringBuilder();

			for(int i=0 ; i < mpSchema.Rows.Count; i++)
			{
				DataRow schemaRow = mpSchema.Rows[i];
				String colname = Quote(Convert.ToString(schemaRow["ColumnName"]));

				if (! IncludedInUpdate(schemaRow)) continue;

				if (setstr.Length > 0) 
					setstr.Append(", ");

				IDbDataParameter p = CreateParameter(schemaRow, false);
				cmd.Parameters.Add(p);

				setstr.Append( colname );
				setstr.Append( "=" );
				setstr.Append( p.ParameterName );
			}

			StringBuilder sb = new StringBuilder();

			sb.Append("UPDATE ");
			sb.Append(Quote(mpTableName));
			sb.Append(" SET ");
			sb.Append(setstr.ToString());
			sb.Append(" WHERE ");
			sb.Append(CreateOriginalWhere(cmd));
			sb.Append("; ");
			sb.Append(CreateFinalSelect(false));

			cmd.CommandText = sb.ToString();

			mpUpdateCmd = cmd;
			return cmd;
		}

		public void RefreshSchema()
		{
			mpSchema = null;

			if( mpInsertCmd != null )
				mpInsertCmd.Dispose();
			mpInsertCmd = null;

			if( mpDeleteCmd != null )
				mpDeleteCmd.Dispose();
			mpDeleteCmd = null;

			if( mpUpdateCmd != null )
				mpUpdateCmd.Dispose();
			mpUpdateCmd = null;

			mpTableName = null;
		}

		private void GenerateSchema()
		{
			if( mpSchema != null )
				return;
			if (mpAdapter == null)
				throw new SQLiteException("Improper SQLiteCommandBuilder state: adapter is null");
			if (mpAdapter.SelectCommand == null)
				throw new SQLiteException("Improper SQLiteCommandBuilder state: adapter's SelectCommand is null");

			IDataReader dr = mpAdapter.SelectCommand.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo);
			mpSchema = dr.GetSchemaTable();
			dr.Close();

			// make sure we got at least one unique or key field and count base table names
			bool   hasKeyOrUnique=false;

			for(int i=0 ; i < mpSchema.Rows.Count; i++)
			{
				DataRow row = mpSchema.Rows[i];
				if (Convert.ToBoolean(row["IsKey"]) || Convert.ToBoolean(row["IsUnique"]))
					hasKeyOrUnique=true;
				if (mpTableName == null)
					mpTableName = Convert.ToString(row["BaseTableName"]);
				else if (mpTableName != Convert.ToString(row["BaseTableName"]))
					throw new InvalidOperationException("SQLiteCommandBuilder does not support multi-table statements");
			}
			if (! hasKeyOrUnique)
				throw new InvalidOperationException("SQLiteCommandBuilder cannot operate on queries with no unique or key columns");
		}

		private String Quote(String table_or_column)
		{
			if (QuotePrefix == null || QuoteSuffix == null)
				return table_or_column;
			return String.Concat(QuotePrefix,table_or_column,QuoteSuffix);
		}

		private IDbDataParameter CreateParameter(DataRow row, bool original)
		{
			String pPrefix;
			DataRowVersion rowVersion;
			if (original)
			{
				pPrefix = OriginalVersionParameterPrefix;
				rowVersion = DataRowVersion.Original;
			}
			else
			{
				pPrefix = "@";
				rowVersion = DataRowVersion.Current;
			}
			IDbDataParameter p = new SQLiteParameter(
				String.Concat(pPrefix,Convert.ToString(row["ColumnName"])), 
				(DbType)Convert.ToInt32(row["ProviderType"]));
			p.SourceColumn = Convert.ToString(row["ColumnName"]);
			p.SourceVersion = rowVersion;
			return p;
		}

		private IDbCommand CreateBaseCommand()
		{
			GenerateSchema();
			IDbCommand cmd = mpAdapter.SelectCommand.Connection.CreateCommand();
			cmd.CommandTimeout = mpAdapter.SelectCommand.CommandTimeout;
			cmd.Transaction = mpAdapter.SelectCommand.Transaction;
			return cmd;
		}

		private string CreateFinalSelect(bool forinsert)
		{
			StringBuilder sel = new StringBuilder();
			StringBuilder where = new StringBuilder();

			foreach (DataRow row in mpSchema.Rows)
			{
				string colname = Convert.ToString(row["ColumnName"]);
				if (sel.Length > 0)
					sel.Append(", ");
				sel.Append( colname );
				if ( !Convert.ToBoolean(row["IsKey"]) ) continue;
				if (where.Length > 0)
					where.Append(" AND ");
				where.Append("(");
				where.Append(colname); 
				where.Append("=");
				if (forinsert) 
				{
					if (Convert.ToBoolean(row["IsAutoIncrement"]))
						where.Append("last_insert_rowid()");
					else if (Convert.ToBoolean(row["IsKey"]))
						where.Append("@" + colname);
				}
				else 
				{
					where.Append(OriginalVersionParameterPrefix);
					where.Append(colname);
				}
				where.Append(")");
			}
			return "SELECT " + sel.ToString() + " FROM " + Quote(mpTableName) +
				" WHERE " + where.ToString();
		}

		private String CreateOriginalWhere(IDbCommand cmd)
		{
			StringBuilder wherestr = new StringBuilder();

			for(int i=0 ; i < mpSchema.Rows.Count; i++)
			{
				DataRow row = mpSchema.Rows[i];
				if (! IncludedInWhereClause(row)) continue;

				// first update the where clause since it will contain all parameters
				if (wherestr.Length > 0)
					wherestr.Append(" AND ");
				String colname = Quote(Convert.ToString(row["ColumnName"]));

				IDbDataParameter op = CreateParameter(row, true);
				cmd.Parameters.Add(op);

				wherestr.Append("(");
				wherestr.Append(colname);
				wherestr.Append("=");
				wherestr.Append(op.ParameterName);
				if (Convert.ToBoolean(row["AllowDBNull"])) 
				{
					wherestr.Append(" OR (");
					wherestr.Append(colname);
					wherestr.Append(" IS NULL AND ");
					wherestr.Append(op.ParameterName);
					wherestr.Append(" IS NULL)");
				}
				wherestr.Append(")");
			}
			return wherestr.ToString();
		}

		private bool IncludedInInsert (DataRow schemaRow)
		{
			if (Convert.ToBoolean(schemaRow["IsAutoIncrement"]))
				return false;
			if (Convert.ToBoolean(schemaRow["IsRowVersion"]))
				return false;
			if (Convert.ToBoolean(schemaRow["IsReadOnly"]))
				return false;
			return true;
		}

		private bool IncludedInUpdate (DataRow schemaRow)
		{
			if (Convert.ToBoolean(schemaRow["IsAutoIncrement"]))
				return false;
			if (Convert.ToBoolean(schemaRow["IsRowVersion"]))
				return false;
			return true;
		}

		private bool IncludedInWhereClause (DataRow schemaRow)
		{
			return true;
		}

		private void SetParameterValues(IDbCommand cmd, DataRow dataRow)
		{
			foreach( IDbDataParameter p in cmd.Parameters )
			{
				if (p.ParameterName.StartsWith(OriginalVersionParameterPrefix))
					p.Value = dataRow[ p.SourceColumn, DataRowVersion.Original ];
				else
					p.Value = dataRow[ p.SourceColumn, DataRowVersion.Current ];
			}
		}

		private void OnRowUpdating(Object sender, RowUpdatingEventArgs args)
		{
			// make sure we are still to proceed
			if (args.Status != UpdateStatus.Continue) return;

			switch( args.StatementType )
			{
				case StatementType.Delete:	args.Command = GetDeleteCommand();	break;
				case StatementType.Update:	args.Command = GetUpdateCommand();	break;
				case StatementType.Insert:	args.Command = GetInsertCommand();	break;
				default:	return;
			}

			SetParameterValues(args.Command, args.Row);
		}
	}
}