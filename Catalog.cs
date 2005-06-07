using System;
using System.Collections;
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

			//In order to perform the search, explore the
			//title word graph, searching out those nodes which can
			//fully account for the search abbreviation.

			SQLiteConnection conn = GetConnection();
			using (SQLiteTransaction tx = conn.BeginTransaction()) {
				//Build the root of the word graph, consisting of all nodes
				//that match the first letter (and possibly additional letters)
				//of the search term
				WordGraphNode root = BuildRootGraphNode(searchTerm);

				//Recurse down the nodes in the tree, evaluating more and more
				//of the search term until it is fully accounted for
				ArrayList completeMatchList = new ArrayList();

				BuildGraphBranch(root, searchTerm, completeMatchList);

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
				}

				using (SQLiteCommand cmd = conn.CreateCommand()) {
					cmd.Transaction = tx;
					cmd.CommandType = CommandType.Text;
					cmd.CommandText = "insert into matching_node_ids(node_id) values (?)";
					cmd.CreateAndAddUnnamedParameters();
					cmd.Prepare();

					foreach (long nodeId in completeMatchList) {
						cmd.Parameters[0].Value = nodeId;
						cmd.ExecuteNonQuery();
					}
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
				}
				using (SQLiteCommand cmd = conn.CreateCommand()) {
					cmd.CommandType = CommandType.Text;
					cmd.CommandText = @"drop table matching_node_ids;";
					cmd.ExecuteNonQuery();
				}

				tx.Commit();

				return results;
			}
		}

		void BuildGraphBranch(WordGraphNode node, String searchTerm, ArrayList completeMatchList) {
			//Populates the children of a given node with title word graph nodes matching the search term
			//node is assumed to have at least one generation of children.
			ArrayList asyncResults = new ArrayList();
			foreach (WordGraphNode childNode in node.ChildNodes) {
				//Each child node is assumed to have been matched with the longest prefix it has in common
				//with the search term.  Thus, further descendants will be matched against what remains
				//of the search term after this prefix is removed, and so on recursively until
				//the entire search term is accounted for.
				//
				//Thus, find the child nodes for childNode starting with the maximal-length prefix
				//accounted for by childNode, then with the max-length prefix minus one, and so on
				//back to one character prefix.
				if (childNode.MatchThisPath == searchTerm) {
					if (!completeMatchList.Contains(childNode.NodeId)) {
						completeMatchList.Add(childNode.NodeId);
					}
					
					//This is a complete match.  There is little point in looking for child
					//nodes using this child node, since there are no more chars in the search
					//term left.  Knock off a character, or if there are no more characters
					//to knock off, do not perform any further processing on this node
					if (childNode.MatchThisWord.Length > 1) {
						childNode.MatchThisWord = childNode.MatchThisWord.Substring(0, childNode.MatchThisWord.Length-1);
					} else {
						continue;
					}
				}

				//As long as there are letters left associated with this child node's match to the 
				//search term, continue looking for matching children
				WordGraphNode workingChildNode = childNode;

				while (workingChildNode.MatchThisWord.Length > 0) {
					//Look for descendents of this node matching some portion of the search
					//term.
					String remainingSearchTerm = searchTerm.Substring(workingChildNode.MatchThisPath.Length);

					using (SQLiteCommand cmd = GetConnection().CreateCommand()) {
						cmd.CommandType = CommandType.Text;
						cmd.CommandText = @"
select distinct twg.node_id, twg.prev_node_id, twg.ordinal, tw.word
from 
	title_word_graph twg
	inner join 
	title_words tw
	on 
	twg.word_id = tw.word_id

	inner join
	title_word_graph_node_descendants desc
	on
	twg.node_id = desc.descendant_node_id
where
	(
		tw.one_chars = ?
		and
		desc.node_id = ?
	)";

						cmd.CreateAndAddUnnamedParameters();

						cmd.Parameters[0].Value = remainingSearchTerm.Substring(0, 1);
						cmd.Parameters[1].Value = workingChildNode.NodeId;

						SQLiteDataReader rdr = cmd.ExecuteReader();
			
						while (rdr.Read()) {
							long nodeId = rdr.GetInt64(0);
							long prevNodeId = NULL_ID;
							if (!rdr.IsDBNull(1)) {
								prevNodeId = rdr.GetInt64(1);
							}
							int ordinal = rdr.GetInt32(2);
							String word = rdr.GetString(3);

							WordGraphNode grandworkingChildNode = workingChildNode.AddChild(nodeId, NULL_ID, ordinal, word);

							//Set the MatchThisNode to the longest prefix of the search term that matches this word
							int prefixLength = remainingSearchTerm.Length;
							while (true) {
								if (word.StartsWith(remainingSearchTerm.Substring(0, prefixLength))) {
									break;
								}

								//Else, doesn't start with that much of the search term; back it off
								prefixLength--;
							}

							grandworkingChildNode.MatchThisWord = remainingSearchTerm.Substring(0, prefixLength);
						}
						rdr.Close();

						//Process these child nodes
						asyncResults.Add(BeginBuildGraphBranch(workingChildNode, searchTerm, completeMatchList));
					}

					//For each iteration of the loop, work on a different working copy of workingChildNode, so threads
					//processing other iterations are not effectec by changes to MatchThisWord
					workingChildNode = workingChildNode.Copy();

					//Remove the right-most character from the match for this node, so children of this node
					//starting with that character can be explored
					workingChildNode.MatchThisWord = workingChildNode.MatchThisWord.Substring(0, workingChildNode.MatchThisWord.Length-1);
				}
			}

			//Wait for the processing of the child graphs
			EndBuildGraphBranch((IAsyncResult[])asyncResults.ToArray(typeof(IAsyncResult)));

			//All children processed; remove them
			node.RemoveAllChildren();
		}

		delegate void BuildGraphBranchDelegate(WordGraphNode node, String searchTerm, ArrayList completeMatchList);
		BuildGraphBranchDelegate _del;

		private IAsyncResult BeginBuildGraphBranch(WordGraphNode node, String searchTerm, ArrayList completeMatchList) {
			if (_del == null ) {	
				_del = new BuildGraphBranchDelegate(BuildGraphBranch);
			}
			return _del.BeginInvoke(node, searchTerm, completeMatchList, null, null);
		}

		private void EndBuildGraphBranch(IAsyncResult[] ars) {
			foreach (IAsyncResult ar in ars) {
				_del.EndInvoke(ar);
			}
		}

		private WordGraphNode BuildRootGraphNode(String searchTerm) {
			//Returns the root graph node with the first generation of child nodes populated
			//with all nodes matching the first letter of the search term
			WordGraphNode root = WordGraphNode.CreateRoot();

			using (SQLiteCommand cmd = GetConnection().CreateCommand()) {
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = @"
select twg.node_id, twg.prev_node_id, twg.ordinal, tw.word
from 
	title_word_graph twg
	inner join 
	title_words tw
	on 
	twg.word_id = tw.word_id
where
	tw.one_chars = ?";

				cmd.CreateAndAddUnnamedParameters();

				cmd.Parameters[0].Value = searchTerm.Substring(0, 1);

				SQLiteDataReader rdr = cmd.ExecuteReader();
			
				while (rdr.Read()) {
					long nodeId = rdr.GetInt64(0);
					long prevNodeId = NULL_ID;
					if (!rdr.IsDBNull(1)) {
						prevNodeId = rdr.GetInt64(1);
					}
					int ordinal = rdr.GetInt32(2);
					String word = rdr.GetString(3);

					WordGraphNode node = root.AddChild(nodeId, NULL_ID, ordinal, word);

					//Set the MatchThisNode to the longest prefix of the search term that matches this word
					int prefixLength = searchTerm.Length;
					while (true) {
						if (word.StartsWith(searchTerm.Substring(0, prefixLength))) {
							break;
						}

						//Else, doesn't start with that much of the search term; back it off
						prefixLength--;
					}

					node.MatchThisWord = searchTerm.Substring(0, prefixLength);
				}
				rdr.Close();
			}

			return root;
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
			return "|" + match.Value;
		}
	}
}
