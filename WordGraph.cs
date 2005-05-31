using System;

namespace Mercury
{
	public class WordGraph
	{
		WordGraphNode _root;

		public WordGraph()
		{
			_root = WordGraphNode.CreateRoot();
		}

		public WordGraphNode RootNode {get {return _root;}}
	}
}
