using System;
using System.Collections.Generic;
using System.Text;

namespace TioClient
{
    public class Connection
    {
        IntPtr _nativeHandle = new IntPtr();

        static Connection()
        {
            NativeImports.tio_initialize();
        }

        public Connection(string host, short port)
        {
            int result;

            result = NativeImports.tio_connect(host, port, out _nativeHandle);
            NativeImports.ThrowOnNativeApiError(result);
        }

        public void Disconnect()
        {
            //NativeImports.tio_disconnect(_nativeHandle);

            _nativeHandle = new IntPtr();
        }

        public void Ping(string host, short port)
        {
            int result;

            result = NativeImports.tio_ping(_nativeHandle, "TioClientClr");
            NativeImports.ThrowOnNativeApiError(result);
        }
    }
}
