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
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER "AS I" AND ANY EXPRESS
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
using System.Text;

namespace Finisar.SQLite
{
	sealed public class SQLiteParameterCollection : CollectionBase, IDataParameterCollection, IDisposable
	{
		internal SQLiteParameterCollection ()
		{
		}

		public SQLiteParameter this[String pParamName]
		{
			get
			{
				int index = IndexOf(pParamName);
				if( index < 0 )
					return null;
				else
					return InnerList[index] as SQLiteParameter;
			}
			set
			{
				throw new NotSupportedException();
			}
		}

		Object IDataParameterCollection.this[String pParamName]
		{
			get
			{
				return this[pParamName];
			}
			set
			{
				this[pParamName] = value as SQLiteParameter;
			}
		}

		public SQLiteParameter this[int index]
		{
			get
			{
				return (SQLiteParameter)(InnerList[index]);
			}
			set
			{
				throw new NotSupportedException();
			}
		}

		public int Add(Object param)
		{
			if( param == null )
				throw new ArgumentNullException();

			if( InnerList.Contains(param) )
				throw new ArgumentException("Parameter is already added into the collection");

			return InnerList.Add((SQLiteParameter)param);
		}

		public SQLiteParameter Add(SQLiteParameter param)
		{
			Add((Object)param);
			return param;
		}

		public SQLiteParameter Add(String parameterName, Object val)
		{
			SQLiteParameter param = Add(parameterName,DbType.String);
			param.Value = val;
			return param;
		}

		public SQLiteParameter Add(String parameterName, DbType dbType)
		{
			return Add(parameterName,dbType,0);
		}

		public SQLiteParameter Add(String parameterName, DbType dbType, int size)
		{
			return Add(parameterName,dbType,size,null);
		}

		public SQLiteParameter Add(String parameterName, DbType dbType, int size, String sourceColumn)
		{
			SQLiteParameter param = new SQLiteParameter(parameterName,dbType);
			param.Size = size;
			if( sourceColumn != null )
				param.SourceColumn = sourceColumn;
			return Add(param);
		}

		public bool Contains (String parameterName)
		{
			return IndexOf(parameterName) >= 0;
		}

		public int IndexOf (String parameterName)
		{
			if( parameterName == null )
				return -1;

			int count = InnerList.Count;
			for( int i=0 ; i < count ; ++i )
			{
				String name = this[i].ParameterName;
				if( name != null && String.Compare(name,parameterName,true,System.Globalization.CultureInfo.InvariantCulture) == 0 )
					return i;
			}
			return -1;
		}

		public void RemoveAt (String parameterName)
		{
			throw new NotSupportedException();
		}

		#region IDisposable Members

		public void Dispose()
		{
			foreach( SQLiteParameter p in this )
			{
				p.Dispose();
			}
		}

		#endregion
	}
} // namespace Finisar
