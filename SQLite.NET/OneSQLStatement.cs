//////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2003-2004, Finisar Corporation
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice,
//   this list of conditions and the following disclaimer. 
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// * Neither the name of the Finisar Corporation nor the names of its
//   contributors may be used to endorse or promote products derived from this
//   software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER "AS IS" AND ANY EXPRESS
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
	/// <summary>
	/// Summary description for OneSQLStatement.
	/// </summary>
	sealed internal class OneSQLStatement
	{
		private SQLiteCommand mCmd;
		private isqlite_vm mVM;
		private String mCmdText;
		private ArrayList mpParamNames;
		private int mUnnamedParametersStartIndex = 0;

		public OneSQLStatement( SQLiteCommand pCmd, String cmdText, ArrayList paramNames )
		{
			mCmd = pCmd;
			mCmdText = cmdText;
			mpParamNames = paramNames;
			if (mCmd == null)
				throw new ArgumentNullException("pCmd");

			if (mCmdText == null)
				throw new ArgumentNullException("cmdText");

			if (mCmdText.Length == 0)
				throw new ArgumentException("The command text must be non-empty");
		}

		public string CommandText
		{
			get { return mCmdText; }
		}

		public void Compile()
		{
			if (mCmd.mpConn == null || mCmd.mpConn.mState == ConnectionState.Closed || mCmd.mpConn.mState == ConnectionState.Broken)
				throw new InvalidOperationException();
			if (mVM != null)
			{
				mVM.reset();
			}
			else
			{
				if (mCmdText.Length == 0)
					throw new InvalidOperationException();
				mVM = mCmd.mpConn.sqlite.compile (mCmdText);
			}

			BindParameters();
		}

		public int GetUnnamedParameterCount()
		{
			int count = 0;
			for( int i=0 ; i < mpParamNames.Count ; ++i )
			{
				if( mpParamNames[i] == null )
					++count;
			}
			return count;
		}

		public void BindParameters()
		{
			int count = mpParamNames.Count;
			int unnamedParamCount = 0;
			for( int i=0 ; i < count ; ++i )
			{
				SQLiteParameter pParam = null;
				String pParamName = (String)mpParamNames[i];
				if( pParamName == null )
				{
					pParam = mCmd.FindUnnamedParameter(unnamedParamCount+mUnnamedParametersStartIndex);
					++unnamedParamCount;
				}
				else
					pParam = mCmd.FindNamedParameter(pParamName);

				mVM.bind(i+1, pParam);
			}
		}

		public void UnCompile ()
		{
			if (mVM != null)
			{
				isqlite_vm pVM = mVM;
				mVM = null;
				pVM.Dispose();
			}
		}

		public isqlite_vm GetVM()
		{
			return mVM;
		}

		public void SetUnnamedParametersStartIndex( int index )
		{
			mUnnamedParametersStartIndex = index;
		}
	}
}
