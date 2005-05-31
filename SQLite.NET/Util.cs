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
using System.Runtime.InteropServices;

namespace Finisar.SQLite
{
	sealed internal class Util
	{
		private const int WinCE = (int)PlatformID.WinCE;
		private const int UNIX = 128;	// from Mono sources

		public static void CompressFile( string filename )
		{
			int platform = (int)Environment.OSVersion.Platform;
			if( platform == WinCE || platform == UNIX )
				return;

			// Set compression on database file (only supported on NTFS)
			IntPtr h = CreateFile(filename,GENERIC_READ|GENERIC_WRITE,FILE_SHARE_READ,(IntPtr)0,OPEN_ALWAYS,FILE_ATTRIBUTE_NORMAL,(IntPtr)0);
			if( h == IntPtr.Zero )
				throw new SQLiteException("Can not open file "+filename);
			UInt16 compressionState = COMPRESSION_FORMAT_DEFAULT;
			UInt32 bytesReturned;
			DeviceIoControl(h,FSCTL_SET_COMPRESSION,ref compressionState,2,(IntPtr)0,0,out bytesReturned,(IntPtr)0);
			CloseHandle(h);
		}

		public static IntPtr AllocateUnmanagedMemory( Int32 size )
		{
			if( (int)Environment.OSVersion.Platform == UNIX )
				return malloc((UInt32)size);
			else
				return HeapAlloc( GetProcessHeap(), 0, (UInt32)size );
		}

		public static void FreeUnmanagedMemory( IntPtr ptr )
		{
			if( (int)Environment.OSVersion.Platform == UNIX )
				free(ptr);
			else
				HeapFree( GetProcessHeap(), 0, ptr );
		}

		public static Int32 StrLen(IntPtr str)
		{
			if( (int)Environment.OSVersion.Platform == UNIX )
				return (Int32)strlen(str);

#if PLATFORM_COMPACTFRAMEWORK
			IntPtr s = memchr(str,0,UInt32.MaxValue);
			return s.ToInt32() - str.ToInt32();
#else
			return lstrlen(str);
#endif
		}


		#region UNIX functions
		[DllImport("libc")]
		private extern static IntPtr malloc(UInt32 bytes);

		[DllImport("libc")]
		private extern static void free(IntPtr lpMem);

		[DllImport("libc")]
		private extern static UInt32 strlen(IntPtr lpMem);
		#endregion

		#region Win32 functions
#if PLATFORM_COMPACTFRAMEWORK
		public const string KERNEL_DLL = "coredll";
#else
		public const string KERNEL_DLL = "kernel32";
#endif

		[DllImport(KERNEL_DLL)]
		private extern static IntPtr GetProcessHeap();

		[DllImport(KERNEL_DLL)]
		private extern static IntPtr HeapAlloc(IntPtr heap, UInt32 flags, UInt32 bytes);

		[DllImport(KERNEL_DLL)]
		private extern static bool HeapFree(IntPtr heap, UInt32 flags, IntPtr lpMem);

#if PLATFORM_COMPACTFRAMEWORK
		[DllImport(KERNEL_DLL)]
		private extern static IntPtr memchr(IntPtr ptr, Int32 c, UInt32 count);
#else
		[DllImport(KERNEL_DLL)]
		private extern static int lstrlen(IntPtr str);
#endif

		private const UInt32 FSCTL_SET_COMPRESSION = 0x9c040;
		private const UInt32 GENERIC_READ = 0x80000000;
		private const UInt32 GENERIC_WRITE = 0x40000000;
		private const UInt32 FILE_SHARE_READ = 0x00000001;
		private const UInt32 OPEN_ALWAYS = 4;
		private const UInt32 FILE_ATTRIBUTE_NORMAL = 0x00000080;
		private const UInt16 COMPRESSION_FORMAT_DEFAULT = 0x0001;

		[DllImport(KERNEL_DLL)]
		private extern static bool DeviceIoControl(
			IntPtr hDevice,
			UInt32 dwIoControlCode,
			ref UInt16 lpInBuffer,
			UInt32 nInBufferSize,
			IntPtr lpOutBuffer,
			UInt32 nOutBufferSize,
			out UInt32 lpBytesReturned,
			IntPtr lpOverlapped
			);

		[DllImport(KERNEL_DLL)]
		private extern static IntPtr CreateFile(
			string lpFileName, 
			UInt32 dwDesiredAccess, 
			UInt32 dwShareMode, 
			IntPtr lpSecurityAttributes, 
			UInt32 dwCreationDispostion, 
			UInt32 dwFlagsAndAttributes, 
			IntPtr hTemplateFile
			); 

		[DllImport(KERNEL_DLL)]
		private extern static bool CloseHandle( IntPtr h );

		#endregion
	}

	/// <summary>
	/// MarshalStr converts between SQLite and .NET strings.
	/// </summary>
	sealed internal class MarshalStr : IDisposable
	{
		private String mpString;
		private IntPtr mhPtr;
		private Int32  mByteLength;
		public	Encoding Encoding;

		public MarshalStr (Encoding encoding) : this(encoding,null)
		{
		}

		public MarshalStr (String str) : this(Encoding.ASCII,str)
		{
		}

		public MarshalStr (Encoding encoding, String str)
		{
			mhPtr = IntPtr.Zero;
			mByteLength = 0;
			Encoding = encoding;
			mpString  = str;
		}

		public static String FromSQLite (IntPtr sqliteStr, Encoding encoding)
		{
			if (sqliteStr == IntPtr.Zero)
				return null;

			Int32 slen = Util.StrLen(sqliteStr);
			Byte[] bytes = new Byte[slen];
			Marshal.Copy (sqliteStr, bytes, 0, slen);
			return encoding.GetString (bytes,0,slen);
		}

		public String Str
		{
			get
			{
				return mpString;
			}
			set
			{
				mpString = value;
				FreeMemory();
			}
		}

		public IntPtr GetSQLiteStr()
		{
			if( mpString == null )
				return IntPtr.Zero;

			if (mhPtr == IntPtr.Zero)
			{
				Byte[] bytes = Encoding.GetBytes (mpString);
				mByteLength = bytes.Length + 1;
				mhPtr = Util.AllocateUnmanagedMemory(mByteLength);
				Marshal.Copy (bytes, 0, mhPtr, bytes.Length);
				Marshal.WriteByte(mhPtr,bytes.Length,0);
			}
			return mhPtr;
		}

		public int GetSQLiteStrByteLength()
		{
			GetSQLiteStr();
			return mByteLength;
		}

		private void FreeMemory()
		{
			if (mhPtr != IntPtr.Zero)
			{
				Util.FreeUnmanagedMemory(mhPtr);
				mhPtr = IntPtr.Zero;
			}
			mByteLength = 0;
		}

		#region IDisposable Members

		public void Dispose()
		{
			FreeMemory();
		}

		#endregion
	}

} // namespace Finisar
