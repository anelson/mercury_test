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
	sealed public class SQLiteTransaction : IDbTransaction
	{
		private SQLiteConnection mpConn;

		public void Dispose ()
		{
			if (mpConn != null && mpConn.mState != ConnectionState.Closed)
				Rollback();
		}

		IDbConnection IDbTransaction.Connection
		{
			get
			{
				return Connection;
			}
		}

		public SQLiteConnection Connection
		{
			get
			{
				return mpConn;
			}
		}

		public IsolationLevel IsolationLevel
		{
			get
			{
				return IsolationLevel.Unspecified;
			}
		}

		public void Commit ()
		{
			CommitOrRollback (true);
		}

		public void Rollback ()
		{
			CommitOrRollback (false);
		}

		private void CommitOrRollback (bool commit)
		{
			if (mpConn == null || mpConn.mState != ConnectionState.Open)
				throw new InvalidOperationException();
			mpConn.sqlite.exec (commit ? "commit transaction" : "rollback transaction");
			mpConn.mpTrans = null;
			mpConn = null;
		}

		internal SQLiteTransaction (SQLiteConnection pConn)
		{
			mpConn = pConn;
			if (pConn == null)
				throw new ArgumentNullException();
			mpConn.sqlite.exec("begin transaction");
		}
	}
} // namespace Finisar
