using System;
using System.Collections;

namespace Mercury
{
	public class SearchResults : CollectionBase {
	    public SearchResult this[ int index ]  {
			get  {
				return( (SearchResult) List[index] );
			}
			set  {
				List[index] = value;
			}
		}

		public int Add( SearchResult value )  {
			return( List.Add( value ) );
		}

		public int IndexOf( SearchResult value )  {
			return( List.IndexOf( value ) );
		}

		public void Insert( int index, SearchResult value )  {
			List.Insert( index, value );
		}

		public void Remove( SearchResult value )  {
			List.Remove( value );
		}

		public bool Contains( SearchResult value )  {
			// If value is not of type SearchResult, this will return false.
			return( List.Contains( value ) );
		}

		protected override void OnInsert( int index, Object value )  {
			if ( value.GetType() != typeof(SearchResult) )
				throw new ArgumentException( "value must be of type SearchResult.", "value" );
		}

		protected override void OnRemove( int index, Object value )  {
			if ( value.GetType() != typeof(SearchResult) )
				throw new ArgumentException( "value must be of type SearchResult.", "value" );
		}

		protected override void OnSet( int index, Object oldValue, Object newValue )  {
			if ( newValue.GetType() != typeof(SearchResult) )
				throw new ArgumentException( "newValue must be of type SearchResult.", "newValue" );
		}

		protected override void OnValidate( Object value )  {
			if ( value.GetType() != typeof(SearchResult) )
				throw new ArgumentException( "value must be of type SearchResult." );
		}

	}
}
