using System;

namespace Mercury
{
	public class SearchResult : IComparable
	{
		Int64 _catId;
		String _title;
		String _uri;
		Single _score;

		public SearchResult(Int64 catId, String title, String uri)
		{
			_catId = catId;
			_title = title;
			_uri = uri;
			_score = 0;
		}

		public Int64 CatId { get {return _catId;} }
		public String Title { get {return _title;} }
		public String Uri { get {return _uri;} }

		public Single Score {
			get {
				return _score;
			}

			set {
				_score = value;
			}
		}

		public override string ToString() {
			return String.Format("{0} ({1})", _title, _uri);
		}
		#region IComparable Members

		public int CompareTo(object obj) {
			SearchResult sr = (SearchResult)obj;

			return _score.CompareTo(sr.Score);
		}

		#endregion
	}
}
