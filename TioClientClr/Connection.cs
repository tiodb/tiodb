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

        public Container Open(string name)
        {
            IntPtr handle = new IntPtr();

            int result = NativeImports.tio_open(_nativeHandle, name, "", out handle);
            NativeImports.ThrowOnNativeApiError(result);

            return new Container(handle, name);
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
