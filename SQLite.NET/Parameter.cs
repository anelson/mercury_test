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
	sealed public class SQLiteParameter : IDbDataParameter, IDisposable
	{
		private DbType mType = DbType.String;
		private String mpName;
		private String mpSourceColumn = "";
		private DataRowVersion mSourceVersion = DataRowVersion.Default;
		private Object mpValue;
		private int mSize = 0;
		private MarshalStr mpMarshalStr = null;

		public SQLiteParameter()
		{
		}

		public SQLiteParameter (String name, DbType type)
		{
			mType  = type;
			mpName = name;
		}

		public DbType DbType
		{
			get
			{
				return mType;
			}
			set
			{
				mType = value;
			}
		}

		public ParameterDirection Direction
		{
			get
			{
				return ParameterDirection.Input;
			}
			set
			{
				if (value != ParameterDirection.Input)
					throw new ArgumentOutOfRangeException();
			}
		}

		public bool IsNullable
		{
			get
			{
				return false;
			}
		}

		public String ParameterName
		{
			get
			{
				return mpName;
			}
			set
			{
				mpName = value;
			}
		}

		public String SourceColumn
		{
			get
			{
				return mpSourceColumn;
			}
			set
			{
				if (value == null)
					throw new ArgumentNullException();
				mpSourceColumn = value;
			}
		}

		public DataRowVersion SourceVersion
		{
			get
			{
				return mSourceVersion;
			}
			set
			{
				mSourceVersion = value;
			}
		}

		public Object Value
		{
			get
			{
				return mpValue;
			}
			set
			{
				mpValue = value;

				if( mpMarshalStr != null )
					mpMarshalStr.Str = null;	// purge cache
			}
		}

		public Byte Precision
		{
			get
			{
				throw new NotSupportedException();
			}
			set
			{
				throw new NotSupportedException();
			}
		}

		public Byte Scale
		{
			get
			{
				throw new NotSupportedException();
			}
			set
			{
				throw new NotSupportedException();
			}
		}

		public int Size
		{
			get
			{
				return mSize;
			}
			set
			{
				mSize = value;
			}
		}

		#region IDisposable Members

		public void Dispose()
		{
			if( mpMarshalStr != null )
			{
				mpMarshalStr.Dispose();
				mpMarshalStr = null;
			}
		}

		#endregion

		internal MarshalStr GetMarshalStr( Encoding encoding )
		{
			if( mpMarshalStr == null )
				mpMarshalStr = new MarshalStr(encoding);
			else
			{
				// purge cache if the encoding is different
				if( mpMarshalStr.Encoding != encoding )
				{
					mpMarshalStr.Encoding = encoding;
					mpMarshalStr.Str = null;
				}
			}
			return mpMarshalStr;
		}
	}
} // namespace Finisar
