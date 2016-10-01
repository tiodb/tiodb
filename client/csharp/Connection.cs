using System;
using System.Collections.Generic;
using System.Text;

namespace TioClient
{
    public class Connection
    {
        IntPtr _nativeHandle = new IntPtr();
        string _host = null;
        short _port = 0;

        static Connection()
        {
            NativeImports.tio_initialize();
        }

        public string Host { get { return _host; } }
        public short Port { get { return _port; } }

        public Container Open(string name)
        {
            IntPtr handle = new IntPtr();

            int result = NativeImports.tio_open(_nativeHandle, name, "", out handle);
            NativeImports.ThrowOnNativeApiError(result);

            return new Container(handle, name);
        }

        public void Close()
        {
            int result = NativeImports.tio_disconnect(_nativeHandle);
        }

        public Connection(string host, short port)
        {
            int result = NativeImports.tio_connect(host, port, out _nativeHandle);
            NativeImports.ThrowOnNativeApiError(result);

            _host = host;
            _port = port;
        }

        public void Disconnect()
        {
            NativeImports.tio_disconnect(_nativeHandle);

            _nativeHandle = new IntPtr();
            _host = null;
            _port = 0;
        }

        public void Ping(string host, short port)
        {
            int result = NativeImports.tio_ping(_nativeHandle, "tioclient");
            NativeImports.ThrowOnNativeApiError(result);
        }
    }
}
