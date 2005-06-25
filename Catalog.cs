using System;
using System.Collections;
using System.Diagnostics;
using System.Reflection;
using System.Data;
using System.Threading;
using System.Text;
using System.Text.RegularExpressions;
using System.IO;

using Finisar.SQLite;

namespace Mercury {
	public class Catalog : IDisposable {
		const long NULL_ID = -1;

		//SQLiteConnection _conn = null;

		[ThreadStatic]
		static SQLiteConnection s_conn;

		Hashtable _wordIdHash;
		Hashtable _tempTableHash;

		public Catalog() {
		}

		#region IDisposable Members

		public void Dispose() {
			/*
			if (_conn != null) {
				_conn.Dispose();
			}
			*/
		}

		#endregion

		public void InitCatalog() {
			if (File.Exists("mercury.db")) {
				File.Delete("mercury.db");
			}

			//Load and run the database initialization script
			TextReader tr = new StreamReader(Assembly.GetExecutingAssembly().GetManifestResourceStream("Mercury.MercDb.sql"));
			using (SQLiteCommand cmd = GetConnection().CreateCommand()) {
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = tr.ReadToEnd();
				tr.Close();

				cmd.ExecuteNonQuery();
			}
		}


		public void ClearCatalog() {
			using (SQLiteCommand cmd = GetConnection().CreateCommand()) {
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = "delete from catalog_items";

				cmd.ExecuteNonQuery();
			}
		}

		public Object BeginBuildCatalog() {
			return GetConnection().BeginTransaction();
		}

		public void EndBuildCatalog(Object obj) {
			((SQLiteTransaction)obj).Commit();
		}
			

		public long AddCatalogItem(String title, String uri) {
			using (SQLiteCommand cmd = GetConnection().CreateCommand()) {
				//using (cmd.Transaction = cmd.Connection.BeginTransaction()) {
					cmd.CommandType = CommandType.Text;
					cmd.CommandText = "insert into catalog_items(title, uri) values(?, ?)";
					cmd.CreateAndAddUnnamedParameters();

					cmd.Parameters[0].DbType = DbType.String;
					cmd.Parameters[0].Value = title;

					cmd.Parameters[1].DbType = DbType.String;
					cmd.Parameters[1].Value = uri;

					cmd.ExecuteNonQuery();

					long catId = cmd.Connection.GetLastInsertRowId();
				
					SetItemTitleWords(cmd.Transaction, catId, title);

					//cmd.Transaction.Commit();

					return catId;
				//}
			}
		}

		public SearchResults SearchCatalog(String searchTerm) {
			SearchResults results = new SearchResults();
			
			//This entire search is case-insensitive
			searchTerm = searchTerm.Trim().ToLower();

			if (searchTerm.Length == 0) {
				//Can't search on empty string
				return results;
			}

			//The search term is applied to the graph of nodes
			//once for each permutation of the search term, where
			//a permutation consists of the original search term with
			//zero or more word separators inserted between the letters.
			//This, for the search term 'cpal', one permutation would match
			//a single word prefixed with 'cpal', while another permutation 
			//might a word prefixed with 'c', followed one or more words later 
			//by a word prefixed with 'pal'.
			//
			//This is conveniently expressed by a bitmap where each bit
			//represents the space between two adjacent letters in the search term
			//A 1 bit indicates the presence of a word separator between the letters,
			//while a 0 bit denotes no word separator.
			//
			//Thus, computing the permutations is as simple as counting from
			//0 to 2^n - 1, where n is the number of spaces between characters
			//in the search term.  This is simply the length of the search term minus
			//1.
			int bitmapLen = searchTerm.Length - 1;
			System.Diagnostics.Trace.Assert(searchTerm.Length < 64);

			//Keep an array list of the separated search terms, 
			//where each element in the array list is an array of strings, 
			//with each string being the prefix of a word in the word graph
			ArrayList searchTermPermutations = new ArrayList();
			ArrayList searchTermBitmaps = new ArrayList();
			
			if (searchTerm.Length != 1) 
			{
				for (ulong bitmap = 0; bitmap < (1UL << bitmapLen); bitmap++) 
				{
					//Count the 1 bits in this value of bitmap */
					ulong w = bitmap;
					int numOnes = 0;
					while (w != 0) 
					{
						numOnes++;
						w &= w - 1;
					}

					//There are numOnes word separators to be inserted, therefore
					//there will be numOnes + 1 substrings as a result
					String[] searchString = new String[numOnes + 1];
					int[] separatorIdxs = new int[numOnes];
					int lastIdx = 0;

					//For each non-zero bit in the bitmap, split the string there
					for (ulong mask = 1, idx = 0; 
						mask <= bitmap; 
						idx++, mask<<=1) 
					{
						if ((bitmap & mask) == mask) 
						{
							//The idx-th bit is set, so split the search term
							//after the idx+1st character
							separatorIdxs[lastIdx++] = (int)idx + 1;						
						}
					}

					//separatorIdxs is a list of the 0-based character positions after
					//which a separator should be inserted.  Thus, 1 denotes separating 
					//the first char from the rest, 2 separates the first two chars, etc
					lastIdx = 0;
					for (int idx = 0; idx < separatorIdxs.Length; idx++) 
					{
						searchString[idx] = searchTerm.Substring(lastIdx,  separatorIdxs[idx] - lastIdx);
						lastIdx = separatorIdxs[idx];
					}
					//The last substring is from the last index plus one, to the end
					//of the search string
					searchString[searchString.Length - 1] = searchTerm.Substring(lastIdx);

					searchTermPermutations.Add(searchString);
					searchTermBitmaps.Add(bitmap);
				}
			} 
			else 
			{
				//Search term is only one char long
				searchTermPermutations.Add(new String[] {searchTerm} );
				searchTermBitmaps.Add(0UL);
			
			}

			SQLiteConnection conn = GetConnection();
			//If the temp table hash exists from the last search, drop
			//all the temp tables listed therein
			if (_tempTableHash != null) {
				using (SQLiteCommand cmd = conn.CreateCommand()) {
					cmd.CommandType = CommandType.Text;
					
					foreach (String tbl in _tempTableHash.Keys) {
					cmd.CommandText = "drop table " + tbl;
						cmd.ExecuteNonQuery();

						Catalog.DumpCommand(cmd);
					}
				}
			}
			_tempTableHash = new Hashtable();
			
            //For each of the permutations, check for matching nodes
			using (SQLiteTransaction tx = conn.BeginTransaction()) {
				//Retrieve all catalog items whose title includes the matching nodes
				//Do this by filling a temp table with the matching node IDs, then
				//doing a join with some other tables mapping the node IDs to catalog
				//items.
				using (SQLiteCommand cmd = conn.CreateCommand()) {
					cmd.Transaction = tx;
					cmd.CommandType = CommandType.Text;
					cmd.CommandText = @"
create temp table matching_node_ids (
	node_id integer primary key
);";
					
					cmd.ExecuteNonQuery();

					Catalog.DumpCommand(cmd);
				}

				for (int idx = 0; idx < searchTermPermutations.Count; idx++) {
					SearchForPermutation((String[])searchTermPermutations[idx],
										 (ulong)searchTermBitmaps[idx]);
				}

				//Temp table of matching node ids is populated
				//Create and populate a temp table to store the catalog item IDs corresponding to these node ids
				using (SQLiteCommand cmd = conn.CreateCommand()) {
					cmd.Transaction = tx;
					cmd.CommandType = CommandType.Text;
					cmd.CommandText = @"
create temp table matching_cat_ids (
	catalog_item_id integer primary key
);";
					cmd.ExecuteNonQuery();

					Catalog.DumpCommand(cmd);
				}
				using (SQLiteCommand cmd = conn.CreateCommand()) {
					cmd.Transaction = tx;
					cmd.CommandType = CommandType.Text;
					cmd.CommandText = @"
insert into matching_cat_ids
	select distinct ni.catalog_item_id as catalog_item_id
	from
	matching_node_ids mn
	inner join
	title_word_graph_node_items ni
	on
	mn.node_id = ni.node_id";
					cmd.ExecuteNonQuery();

					Catalog.DumpCommand(cmd);
				}

				//Temp table of catalog_item_ids is populated.  Join with the catalog item table
				using (SQLiteCommand cmd = conn.CreateCommand()) {
					cmd.Transaction = tx;
					cmd.CommandType = CommandType.Text;
					cmd.CommandText = @"
select ci.catalog_item_id, ci.title, ci.uri
from catalog_items ci inner join matching_cat_ids
on ci.catalog_item_id = matching_cat_ids.catalog_item_id;";
					SQLiteDataReader rdr = cmd.ExecuteReader();

					Catalog.DumpCommand(cmd);
			
					while (rdr.Read()) {
						long catId = rdr.GetInt64(0);
						String title = rdr.GetString(1);
						String uri = rdr.GetString(2);

						results.Add(new SearchResult(catId, title, uri));
					}
					rdr.Close();
				}

				//Results are retrieved.  Drop the temp tables and return
				using (SQLiteCommand cmd = conn.CreateCommand()) {
					cmd.CommandType = CommandType.Text;
					cmd.CommandText = @"drop table matching_cat_ids;";
					cmd.ExecuteNonQuery();

					Catalog.DumpCommand(cmd);
				}
				using (SQLiteCommand cmd = conn.CreateCommand()) {
					cmd.CommandType = CommandType.Text;
					cmd.CommandText = @"drop table matching_node_ids;";
					cmd.ExecuteNonQuery();

					Catalog.DumpCommand(cmd);
				}

				tx.Commit();

				return results;
			}
		}

        /// <summary>Finds all catalog items whose name matches the specified search terms.  That is,
        ///     the catalog items contain words matching the prefixes in the searchTerms array,
        ///     in the same order as the prefixes in the array, although not necessarily contiguous.
        /// 
        ///     For each matching catalog item, inserts a row into the temp table matching_node_ids, 
        ///     which is assumed to be previously created for that purpose.</summary>
        /// 
        /// <param name="searchTerms"></param>
		void SearchForPermutation(String[] searchTerms, ulong bitmap) {
			SQLiteConnection conn = GetConnection();

			//Create and populate the table with the matches for these
			//terms.  This method will recursively build tables for all
			//steps up to the last search term in the array.
			//Thus, the table name returned by this method contains all
			//matches for this particular combination
			String tableName = PopulateSearchTable(searchTerms);

			using (SQLiteCommand cmd = conn.CreateCommand()) {
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = @"
insert into matching_node_ids
select distinct a.node_id from " + tableName + @" a
left join matching_node_ids mni
on a.node_id = mni.node_id
where mni.node_id is null";
				cmd.ExecuteNonQuery();

				Catalog.DumpCommand(cmd);
			}
		}

		String PopulateSearchTable(String[] searchTerms) {
			String tableName = null;
			StringBuilder sb = new StringBuilder();
			SQLiteConnection conn = GetConnection();

			//The name of the table containing the results of this search
			//is the concatenation of the lenghts of each of the search terms, 
			//which uniquely identifies this particular permutation
			tableName = "tmp_search_tbl";
			foreach (String term in searchTerms) {
				tableName += "_" + term.Length.ToString();
			}

			//If the named temp table hasn't been created yet, create and populate it
			//Todo: if this population is to happen in multiple threads, need to 
			//create a hash table for each connection, as temp tables are only
			//visible to a single connection, and thus thread
			if (_tempTableHash.Contains(tableName)) {
				return tableName;
			}
			
			//Create the table
			using (SQLiteCommand cmd = conn.CreateCommand()) {
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = @"
create temp table " + tableName + @" (
node_id integer primary key
);";
				cmd.ExecuteNonQuery();

				Catalog.DumpCommand(cmd);
			}

			//Prepare the insert statement to populate this table 
			//of results
			sb.AppendFormat("insert into {0} (node_id) ",
							tableName);
			
			if (searchTerms.Length > 1) {
				//Ensure that the temp tables containing provisional results upon
				//which this bitmap depends are extant and populated
				ArrayList preceedingTerms = new ArrayList(searchTerms);
				preceedingTerms.RemoveAt(preceedingTerms.Count - 1);
				
				String prevTable = PopulateSearchTable((String[])preceedingTerms.ToArray(typeof(String)));

				//Find all nodes descended from the nodes in prevTable, 
				//with words starting with the last search term in the array
				sb.AppendFormat(@"
select distinct twg.node_id
from 
	title_word_graph_node_descendants desc
	inner join
	{0} src
	on src.node_id = desc.node_id

	inner join
	title_word_graph twg
	on twg.node_id = desc.descendant_node_id
	
	inner join 
	title_words tw
	on 
	twg.word_id = tw.word_id
",
								prevTable);
			} else {
				//Else, this is the first search term of the search, so scour all catalog words
				//for a match				
				sb.Append(@"
select distinct twg.node_id
from 
	title_word_graph twg
	inner join 
	title_words tw
	on 
	twg.word_id = tw.word_id
");
			}

			//Either way, build the WHERE clause to match the text of the
			//search term.
			//The database pre-computes the substrings consisting of the first
			//one through five chars of each title word, making for more efficient
			//retrieval.  Use the appropriate column, and for words longer
			//than 5 chars, add an additional predicate to match the entire search term
			String lastSearchTerm = searchTerms[searchTerms.Length-1];
			sb.Append(" WHERE ");
			if (lastSearchTerm.Length == 1) {
				sb.Append("tw.one_chars = ?");
			} else if (lastSearchTerm.Length == 2) {
				sb.Append("tw.two_chars = ?");
			} else if (lastSearchTerm.Length == 3) {
				sb.Append("tw.three_chars = ?");
			} else if (lastSearchTerm.Length == 4) {
				sb.Append("tw.four_chars = ?");
			} else if (lastSearchTerm.Length == 5) {
				sb.Append("tw.five_chars = ?");
			} else {
				sb.Append("tw.five_chars = ? and tw.word LIKE ?");
			} 

			using (SQLiteCommand cmd = GetConnection().CreateCommand()) {
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = sb.ToString();

				cmd.CreateAndAddUnnamedParameters();

				if (lastSearchTerm.Length <= 5) {
					//Search term fits w/ one of the prefixes
					cmd.Parameters[0].Value = lastSearchTerm;
				} else {
					//search term too big to fit one of the prefixes.
					//Search on both the first five chars of the term, then 
					//the entire term
					cmd.Parameters[0].Value = lastSearchTerm.Substring(0,  5);
					cmd.Parameters[1].Value = lastSearchTerm + "%";
				}

				//Run the command to populate the table
				cmd.ExecuteNonQuery();

				Catalog.DumpCommand(cmd);
			}			

			//Update the hashtable to reflect that this table is populated
			_tempTableHash.Add(tableName,  true);
			
			return tableName;
		}
		
		private void SetItemTitleWords(SQLiteTransaction tx, long catId, String title) {
			//Tokenize the title into lower-case 'words' based on the word breaking algorithm
			//below, and store each word in the database along with its ordinal position in the
			//title
			String[] titleTokens = TokenizeTitle(title);

			using (SQLiteCommand cmd = tx.Connection.CreateCommand()) {
				cmd.Transaction = tx;
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = "insert into catalog_item_title_words(catalog_item_id, ordinal, word_id) values(?, ?, ?)";
				cmd.CreateAndAddUnnamedParameters();
				cmd.Prepare();

				for (int idx = 0; idx < titleTokens.Length; idx++) {
					cmd.Parameters[0].Value = catId;
					cmd.Parameters[1].Value = idx+1;
					cmd.Parameters[2].Value = GetWordId(titleTokens[idx], true);

					cmd.ExecuteNonQuery();
				}
			}

			//Now, update the catalog-wide title word graph
			//with information from this title
			AddItemTitleToTitleWordGraph(catId, titleTokens);
		}

		private long GetWordId(String word, bool addToList) {
			//Queries the table of title words to get the numeric ID assigned to this word
			//If the word is not found, adds it
			if (_wordIdHash.Contains(word)) {
				return (long)_wordIdHash[word];
			}

			if (!addToList) {
				return NULL_ID;
			}

			//Haven't encountered this word before.  Add it to the list
			using (SQLiteCommand cmd = GetConnection().CreateCommand()) {
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = @"insert into title_words(word, one_chars, two_chars, three_chars, four_chars, five_chars) 
values(?, ?, ?, ?, ?, ?)";
				cmd.CreateAndAddUnnamedParameters();

				cmd.Parameters[0].DbType = DbType.String;
				cmd.Parameters[0].Value = word;

				cmd.Parameters[1].DbType = DbType.String;
				cmd.Parameters[1].Value = word.Substring(0, 1);

				if (word.Length > 1) {
					cmd.Parameters[2].DbType = DbType.String;
					cmd.Parameters[2].Value = word.Substring(0, 2);

					if (word.Length > 2) {
						cmd.Parameters[3].DbType = DbType.String;
						cmd.Parameters[3].Value = word.Substring(0, 3);

						if (word.Length > 3) {
							cmd.Parameters[4].DbType = DbType.String;
							cmd.Parameters[4].Value = word.Substring(0, 4);

							if (word.Length > 4) {
								cmd.Parameters[5].DbType = DbType.String;
								cmd.Parameters[5].Value = word.Substring(0, 5);
							}
						}
					}
				}

				cmd.ExecuteNonQuery();

				long wordId = cmd.Connection.GetLastInsertRowId();

				_wordIdHash.Add(word, wordId);

				return wordId;
			}

		}
		
		private void AddItemTitleToTitleWordGraph(long catId, String[] titleTokens) {
			//Updates the catalog-wide title word graph to include this title
			//Then associates this catalog item with all the nodes along the path of this title
			long prevNodeId = NULL_ID, wordId = NULL_ID;

			//Prepare the command used to add a row in the title_word_graph_node_items associating
			//each of the title word graph nodes along this title, with this catalog item
			using (SQLiteCommand cmd = GetConnection().CreateCommand()) {
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = "insert into title_word_graph_node_items(node_id, catalog_item_id) values (?, ?)";
				cmd.CreateAndAddUnnamedParameters();
				cmd.Prepare();
				cmd.Parameters[1].Value = catId;

				for (int idx = 0; idx < titleTokens.Length; idx++) {
					wordId = GetWordId(titleTokens[idx], true);

					long nodeId = SetTitleWordGraphNode(wordId, prevNodeId, idx+1);

					cmd.Parameters[0].Value = nodeId;
					cmd.ExecuteNonQuery();

					prevNodeId = nodeId;
				}
			}
		}

		private long SetTitleWordGraphNode(long wordId, long prevNodeId, int ordinal) {
			//If the specified word graph node does not exist, builds it.
			long nodeId = NULL_ID;

			using (SQLiteCommand cmd = GetConnection().CreateCommand()) {
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = "select node_id from title_word_graph where word_id = ? and ordinal = ?";
				if (prevNodeId == NULL_ID) {
					cmd.CommandText += " and prev_node_id is null";
				} else {
					cmd.CommandText += " and prev_node_id = ?";
				}
				cmd.CreateAndAddUnnamedParameters();
				cmd.Parameters[0].Value = wordId;
				cmd.Parameters[1].Value = ordinal;
				if (prevNodeId != NULL_ID) {
					cmd.Parameters[2].Value = prevNodeId;
				}

				Object result = cmd.ExecuteScalar();

				if (result != null) {
					nodeId = (long)result;
				}
			}

			if (nodeId == NULL_ID) {
				//This node doesn't exist; create it
				using (SQLiteCommand cmd = GetConnection().CreateCommand()) {
					cmd.CommandType = CommandType.Text;
					cmd.CommandText = "insert into title_word_graph(word_id, prev_node_id, ordinal) values (?,?,?)";
					cmd.CreateAndAddUnnamedParameters();
					cmd.Parameters[0].Value = wordId;
					if (prevNodeId == NULL_ID) {
						cmd.Parameters[1].Value = DBNull.Value;
					} else {
						cmd.Parameters[1].Value = prevNodeId;
					}
					cmd.Parameters[2].Value = ordinal;

					cmd.ExecuteNonQuery();

					nodeId = cmd.Connection.GetLastInsertRowId();
				}

				//Add this node to the list of descendants for all of this node's ancestors
				if (prevNodeId != NULL_ID) {
					AddDescendantNode(prevNodeId, nodeId);
				}
			}

			return nodeId;
		}

		private void AddDescendantNode(long prevNodeId, long nodeId) {
			//Adds nodeId to the list of descendants for prevNodeId, then
			//looks up prevNodeId's parent and add's nodeId to the parent's list of descendants, 
			//and so on, recursively up to the root
			using (SQLiteCommand cmd = GetConnection().CreateCommand()) {
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = "insert into title_word_graph_node_descendants(node_id, descendant_node_id) values (?,?)";
				cmd.CreateAndAddUnnamedParameters();

				cmd.Parameters[0].Value = prevNodeId;
				cmd.Parameters[1].Value = nodeId;

				cmd.ExecuteNonQuery();
			}

			//Ok, nodeId added to prevNodeId's descendants.  Now find prevNodeId's parent and add nodeId there
			using (SQLiteCommand cmd = GetConnection().CreateCommand()) {
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = "select prev_node_id from title_word_graph where node_id = ?";
				cmd.CreateAndAddUnnamedParameters();

				cmd.Parameters[0].Value = prevNodeId;

				Object result = cmd.ExecuteScalar();

				if (result != DBNull.Value) {
					prevNodeId = (long)result;

					AddDescendantNode(prevNodeId, nodeId);
				}
			}
		}

		private SQLiteConnection GetConnection() {
			/*
			if (_conn == null) {
				String connStr = "Data Source=mercury.db;Compress=False;UTF8Encoding=True;Version=3";
				if (!File.Exists("mercury.db")) {
					connStr += ";New=True";
				}
				_conn = new SQLiteConnection (connStr) ;
				_conn.Open();
				_wordIdHash = new Hashtable();
			}

			return _conn;
			*/
			if (Catalog.s_conn== null) {
				String connStr = "Data Source=mercury.db;Compress=False;UTF8Encoding=True;Version=3";
				if (!File.Exists("mercury.db")) {
					connStr += ";New=True";
				}
				Catalog.s_conn = new SQLiteConnection (connStr) ;
				Catalog.s_conn.Open();
			}

			if (_wordIdHash == null) {
				_wordIdHash = new Hashtable();
			}

			return Catalog.s_conn;
		}

		private String[] TokenizeTitle(String title) {
			ArrayList alWords = new ArrayList();

			//Use a regex to pick out the words
			//Use the unicode categories from http://www.unicode.org/Public/UNIDATA/UCD.html#General_Category_Values
			//
			//A word is a contiguous collection of characters in one of the following classes:
			// Lu/Ll/Lt/Lm/Lo - Letters
			// Nd/Nl/No - Numbers
			// Pc/Pd/Ps/Pe/Pi/Pf/Po - Punctuation
			// Sm/Sc/Sk/So - Symbols
			//
			// Within a group of letters, additional words are distinguished based on case.  The following
			// combinations are recognized as distinct words:
			//	A string of all upper-case or all lower-case letters, mixed with optional Lm/Lo characters
			//	A string with a leading upper-case character followed by lower case/Lm/Lo characters
			//  Multiple upper-case followed by Ll/Lm/Lo is interpreted as two words; all but the last
			//	 upper-case character form the first word, while the right-most upper-case character
			//	 and the following Ll/Lm/Lo characters form the second word
			//
			//All other characters are ignored for the purposes of building the list of words
			//
			//Examples:
			//  'foo bar baz' - {'foo', 'bar', 'baz'}
			//  '1234 sucks!' - {'1234', 'sucks', '!'}
			//  'more fuckin' $$$!!!' - {'more', 'fuckin', ''', '$$$', '!!!'}
			//  'fear!s the m1nd-k1ller' - {'fear', '!', 's', 'the', 'm', '1', 'nd', '-', 'k', '1', 'ller'}

			const String LETTERS_GROUP = @"[\p{Lu}\p{Ll}\p{Lt}\p{Lm}\p{Lo}]";
			const String NUMBERS_GROUP = @"[\p{Nd}\p{Nl}\p{No}]";
			const String PUNCT_GROUP = @"[\p{Pc}\p{Pd}\p{Ps}\p{Pe}\p{Pi}\p{Pf}\p{Po}]";
			const String SYM_GROUP = @"[\p{Sm}\p{Sc}\p{Sk}\p{So}]";

			Regex re = new Regex(String.Format("{0}+|{1}+|{2}+|{3}+", LETTERS_GROUP, NUMBERS_GROUP, PUNCT_GROUP, SYM_GROUP));

			MatchCollection matches = re.Matches(title);

			foreach (Match match in matches) {
				//If this is a letters match, do additional word breaking based on case
				//TODO: this could be more efficient by checking the match group the expression matched
				if (Regex.IsMatch(match.Value, LETTERS_GROUP + "+")) {
					String word = match.Value;

					//Insert '|' to denote word-breaks wherever a telltale wordbreak appears

					//First, break at every Upper-to-lower transition
					word = Regex.Replace(word, @"\p{Lu}[\p{Lm}\p{Lo}]*\p{Ll}", new MatchEvaluator(WordBreakMatchEvaluator));

					//Next, break at any Lower-to-upper transitions
					word = Regex.Replace(word, @"\p{Ll}[\p{Lm}\p{Lo}]*\p{Lu}", new MatchEvaluator(WordBreakMatchEvaluator));

					//Now, split the 'word' into the component words, and add them to the word list
					String[] words = word.Split('|');
					foreach (String splitWord in words) {
						//There will be empty strings when the word break is placed at the beginning of 
						//the string; don't include those obviously
						if (splitWord != String.Empty) {
							alWords.Add(splitWord.ToLower());
						}
					}
				} else {
					alWords.Add(match.Value.ToLower());
				}
			}

			return (String[])alWords.ToArray(typeof(String));
		}

		private String WordBreakMatchEvaluator(Match match) {
			//Called when doing Regex.Replace operations in TokenizeTitle to insert word breaks
			//into the string

			//If the first letter of the match is upper case, break before the first
			//letter.  If the first letter of the match is lower case, break after
			//the first letter
			if (Regex.IsMatch(match.Value, @"^\p{Ll}")) {
				//This is a lower-to-upper transition, so insert the break after the
				//lower case char
				//TODO: Is this assumption that the lower-case char is one
				//char long valid for all unicode glyphs?
				return match.Value.Substring(0, 1) + "|" + match.Value.Substring(1);
			} else {
				return "|" + match.Value;
			}
		}

		private static void DumpCommand(SQLiteCommand cmd) {
			#if DEBUG
			StringBuilder sb = new StringBuilder();

			sb.Append(cmd.CommandText);
			sb.Append(Environment.NewLine);
			sb.Append(Environment.NewLine);

			//Expand placeholders
			int placeholderIdx, paramNum = 0;
			while ((placeholderIdx = sb.ToString().IndexOf("?")) != -1) {
				if (cmd.Parameters[paramNum].Value is String) {
					sb.Replace("?",  "'" + cmd.Parameters[paramNum++].Value.ToString() + "'",  placeholderIdx,  1);
				} else {
					sb.Replace("?",  cmd.Parameters[paramNum++].Value.ToString(),  placeholderIdx,  1);
				}
			}

			Debug.WriteLine(sb.ToString());
			#endif
		}
	}
}
