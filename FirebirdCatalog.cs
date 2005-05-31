using System;
using System.Collections;
using System.Reflection;
using System.Data;
using System.Text;
using System.IO;

using FirebirdSql.Data.Firebird;
using FirebirdSql.Data.Firebird.Isql;

namespace Mercury
{
	public class FirebirdCatalog
	{
		public FirebirdCatalog()
		{
		}

		public void InitCatalog() {
			//Create the database, replacing if it exists
			Hashtable parms = new Hashtable();
			parms["User"] = "SYSDBA";
			parms["Password"] = "masterkey";
			parms["Database"] = "mercury.fdb";
			parms["ServerType"] = 1;
			parms["Overwrite"] = true;
			parms["Dialect"] = 3;
			parms["Charset"] = "UNICODE_FSS";
			FbConnection.CreateDatabase(parms);

			//Open a connection to the new database, and populate it with the schema elements
			FbConnection conn = GetConnection();
			FbBatchExecution fbe = new FbBatchExecution(conn);
			Assembly asm = Assembly.GetExecutingAssembly();

			//Load and run the script to create the database
			TextReader tr = new StreamReader(asm.GetManifestResourceStream("Mercury.MercDb.sql"));
			FbScript fbs = new FbScript(tr);
			fbs.Parse();
			fbe.SqlStatements = fbs.Results;
			fbe.Execute();
			tr.Close();
			
			tr = new StreamReader(asm.GetManifestResourceStream("Mercury.MercDbProcs.sql"));
			fbs = new FbScript(tr);
			fbs.Parse();
			fbe.SqlStatements = fbs.Results;
			fbe.Execute();
			tr.Close();

			conn.Close();
		}


		public void ClearCatalog() {
			FbConnection conn = GetConnection();
			try {
				using (FbCommand cmd = new FbCommand("delete from catalog_items", conn)) {
					cmd.ExecuteNonQuery();
				}
			} finally {
				conn.Close();
			}
		}

		public int AddCatalogItem(String title, String uri) {
			FbConnection conn = GetConnection();
			try {
				using (FbCommand cmd = new FbCommand()) {
					using (cmd.Transaction = conn.BeginTransaction(FbTransactionOptions.Autocommit|FbTransactionOptions.Concurrency)) {
						cmd.Connection = conn;
						cmd.CommandType = CommandType.StoredProcedure;
						cmd.CommandText = "sp_AddCatalogItem";
						cmd.Parameters.Add("@title", FbDbType.VarChar, 4000);
						cmd.Parameters.Add("@uri", FbDbType.VarChar, 4000);
						cmd.Parameters.Add("@catalog_item_id", FbDbType.Numeric);

						cmd.Parameters[0].Value = title;
						cmd.Parameters[1].Value = uri;
						cmd.Parameters[2].Direction = ParameterDirection.ReturnValue;

						//Capture the catalog item ID, and build the list
						//of title characters
						cmd.ExecuteNonQuery();

						//int catId = (int)cmd.ExecuteScalar();
						int catId = (int)cmd.Parameters[2].Value;

						SetItemTitleChars(cmd.Transaction, catId, title);

						cmd.Transaction.Commit();

						return catId;
					}
				}
			} finally {
				conn.Close();
			}
		}

		public SearchResults SearchCatalog(String searchTerm) {
			//This entire search is case-insensitive
			searchTerm = searchTerm.ToLower();

			//First, query the catalog for all items that have all of the
			//letters from the search term in their titles.
			StringBuilder sb = new StringBuilder();

			sb.Append(@"
select distinct ci.id, ci.title, ci.uri
from
	catalog_items ci,
	catalog_item_title_chars citc
where
	citc.title_ci_char in ('");
			
			sb.Append(String.Join("','", GetUniqueChars(searchTerm)));
			sb.Append("')");
			
			FbConnection conn = GetConnection();
			try {
				using (FbCommand cmd = new FbCommand()) {
					using (cmd.Transaction = conn.BeginTransaction(FbTransactionOptions.Autocommit|FbTransactionOptions.Concurrency|FbTransactionOptions.Read)) {
						cmd.Connection = conn;
						cmd.CommandType = CommandType.Text;
						cmd.CommandText = sb.ToString();

						FbDataReader rdr = cmd.ExecuteReader();
				
						//For each matching id/title combination, perform
						//a second pass filter, ensuring that
						//the characters in the search term
						//are present in the same order as the
						//search term.  Thus, a search for 'foo'
						//would match 'FOrest Orgy' and 'Fucking asshOle mOment', but
						//not 'Orgy in a FOrest'.
						SearchResults results = new SearchResults();

						while (rdr.Read()) {
							int catId = rdr.GetInt32(0);
							String title = rdr.GetString(1);
							String uri = rdr.GetString(2);

							if (DoesTitleMatchSearch(title, searchTerm)) {
								//This one matches
								results.Add(new SearchResult(catId, title, uri));
							}
						}

						rdr.Close();

						return results;
					}
				}
			} finally {
				conn.Close();
			}
		}

		private void SetItemTitleChars(FbTransaction tx, int catId, String title) {
			using (FbCommand cmd = new FbCommand()) {
				cmd.Connection = tx.Connection;
				cmd.Transaction = tx;
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandText = "sp_AddCatalogItemTitleChar";
				cmd.Parameters.Add("@v_catalog_item_id", FbDbType.Numeric);
				cmd.Parameters.Add("@v_title_char", FbDbType.Char, 1);
				cmd.Prepare();

				//Build a hash table of the unique lower-case characters from the title
				foreach (String c in GetUniqueChars(title)) {
					cmd.Parameters[0].Value = catId;
					cmd.Parameters[1].Value = c;

					cmd.ExecuteNonQuery();
				}
			}
		}

		private String[] GetUniqueChars(String term) {
			Hashtable ht = new Hashtable();

			foreach (Char c in term.ToCharArray()) {
				if (!ht.Contains(c)) {
					ht.Add(c, null);
				}
			}

			String[] chars = new String[ht.Count];
			int idx = 0;

			foreach (Char c in ht.Keys) {
				chars[idx++] = c.ToString();
			}

			return chars;
		}

		private bool DoesTitleMatchSearch(String title, String searchTerm) {
			//For each character in searchTerm (assumed to be converted to lowercase),
			//ensure title (converted to lower case) contains that character, in the same
			//order as the characters appear in the search term.
			//
			//Thus, given a search term 'foo', the title must match the following regex:
			//.*f.*o.*o.*
			//
			//For performance reasons, a regex isn't used, but the result is equivalent
			title = title.ToLower();

			int searchIdx = 0, titleIdx = 0;

			while (searchIdx < searchTerm.Length) {
				//Ensure the title has searchTerm[searchIdx], on or after titleIdx
				if ((titleIdx = title.IndexOf(searchTerm[searchIdx], titleIdx)) == -1) {
					//Title is missing this character
					return false;
				}

				searchIdx++;
			}

			//All search terms were found in the correct order.  This title 
			//tentatively matches the search terms
			return true;
		}

		private FbConnection GetConnection() {
			string connectionString =
				"User=SYSDBA;"                  +
				"Password=masterkey;"           +
				@"Database=mercury.fdb;"  +
				//"DataSource=localhost;"         +
				"Charset=UNICODE_FSS;"                 +
				"Pooling=true;"                 +
				"ServerType=1;";

			FbConnection conn = new FbConnection(connectionString.ToString());
			conn.Open();

			return conn;
		}
	}
}
