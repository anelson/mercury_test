using System;
using System.Collections;
using System.Collections.Specialized;

namespace Mercury
{
	public class WordGraphNode
	{
		long _nodeId;
		long _wordId;
		int _ordinal;
		String _word;
		ArrayList _childNodes;
		WordGraphNode _ancestorNode;
		String _matchThisWord;

		private WordGraphNode()
		{
			_nodeId = -1;
			_wordId = -1;
			_ordinal = 0;
			_word = null;
			_ancestorNode = null;

			_matchThisWord = null;

			_childNodes = new ArrayList();
		}

		private WordGraphNode(long nodeId, long wordId, WordGraphNode ancestorNode, int ordinal, String word) : this() {
			_nodeId = nodeId;
			_wordId = wordId;
			_ancestorNode = ancestorNode;

			_ordinal = ordinal;
			_word = word;
		}

		public long NodeId {get {return _nodeId;}}
		public long WordId {get {return _wordId;}}
		public int Ordinal {get {return _ordinal;}}
		public String Word {get {return _word;}}
		public IEnumerable ChildNodes {get {return _childNodes;}}
		public WordGraphNode AncestorNode {get {return _ancestorNode;}}

		public String MatchThisWord {get {return _matchThisWord;} set {_matchThisWord = value;} }
		public String MatchThisPath {get {return _ancestorNode == null ? _matchThisWord : _ancestorNode.MatchThisPath + _matchThisWord;} }

		public static WordGraphNode CreateRoot() {
			return new WordGraphNode();
		}

		public WordGraphNode AddChild(long nodeId, long wordId, int ordinal, String word) {
			WordGraphNode child = CreateChild(nodeId, wordId, ordinal, word);

			_childNodes.Add(child);

			return child;
		}

		public WordGraphNode CreateChild(long nodeId, long wordId, int ordinal, String word) {
			WordGraphNode child = new WordGraphNode(nodeId, wordId, this, ordinal, word);

			return child;
		}

		public void RemoveChild(WordGraphNode child) {
			_childNodes.Remove(child);
		}

		public void RemoveAllChildren() {
			_childNodes.Clear();
		}
	}
}
