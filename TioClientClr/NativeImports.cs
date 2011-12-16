using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;

namespace TioClient
{
    class NativeImports
    {

        //#define TIO_DATA_TYPE_NONE	 			0x1
        //#define TIO_DATA_TYPE_STRING 			0x2
        //#define TIO_DATA_TYPE_INT 				0x3
        //#define TIO_DATA_TYPE_DOUBLE 			0x4
        const uint TIO_DATA_TYPE_NONE = 1;
        const uint TIO_DATA_TYPE_STRING = 2;
        const uint TIO_DATA_TYPE_INT = 3;
        const uint TIO_DATA_TYPE_DOUBLE = 4;

//struct TIO_DATA
//{	
//    unsigned int data_type;
//    int int_;
//    char* string_;
//    unsigned int string_size_;
//    double double_;
//};
        [StructLayout(LayoutKind.Sequential)]
        public struct TIO_DATA
        {
            public uint data_type;
            public int int_;
            public IntPtr string_;
            uint string_size_;
            public double double_;
        }

        public class TioDataConverter : IDisposable
        {
            // must be public because it will be passed as out/ref parameter
            public TIO_DATA _tiodata;

            public TioDataConverter(TIO_DATA tiodata)
            {
                _tiodata = tiodata;
            }

            public TioDataConverter()
            {
                _tiodata.data_type = TIO_DATA_TYPE_NONE;
            }

            public TioDataConverter(int value)
            {
                _tiodata.data_type = TIO_DATA_TYPE_INT;
                _tiodata.int_ = value;
            }

            public TioDataConverter(string value)
            {
                _tiodata.data_type = TIO_DATA_TYPE_NONE;

                byte[] utf8string = Encoding.UTF8.GetBytes(value);
                GCHandle pinnedArray = GCHandle.Alloc(utf8string, GCHandleType.Pinned);
                IntPtr pointer = pinnedArray.AddrOfPinnedObject();
               
                tiodata_set_string_and_size(ref _tiodata, pointer, utf8string.Length);

                pinnedArray.Free();
            }

            public TioDataConverter(double value)
            {
                _tiodata.data_type = TIO_DATA_TYPE_DOUBLE;
                _tiodata.double_ = value;
            }

            static public TioDataConverter FromObject(object value)
            {
                if (value == null)
                    return new TioDataConverter();
                else if (value is int)
                    return new TioDataConverter((int)value);
                else if (value is string)
                    return new TioDataConverter((string)value);
                else if (value is float || value is double)
                    return new TioDataConverter((double)value);
                else if (value is Decimal)
                    return new TioDataConverter(Decimal.ToDouble((Decimal)value));
                else
                    throw new ArgumentException("unsupported type");
            }

            public static object ToObject(TIO_DATA tiodata)
            {
                object ret;
                switch (tiodata.data_type)
                {
                    case TIO_DATA_TYPE_NONE:
                        ret = null;
                        break;
                    case TIO_DATA_TYPE_STRING:
                        ret = Marshal.PtrToStringAnsi(tiodata.string_);
                        break;
                    case TIO_DATA_TYPE_INT:
                        ret = tiodata.int_;
                        break;
                    case TIO_DATA_TYPE_DOUBLE:
                        ret = tiodata.double_;
                        break;
                    default:
                        throw new Exception("INTERNAL ERROR, invalid TIO_DATA");
                }

                return ret;
            }

            public object AsObject()
            {
                return ToObject(_tiodata);
            }

            public override string ToString()
            {
                return AsObject().ToString();
            }

            void Free()
            {
                if (_tiodata.data_type == TIO_DATA_TYPE_STRING)
                    NativeImports.tiodata_set_as_none(ref _tiodata);
                else
                    _tiodata.data_type = TIO_DATA_TYPE_NONE;
            }

            void IDisposable.Dispose()
            {
                Free();
            }
        }
       
        //void tiodata_set_as_none(struct TIO_DATA* tiodata);
        [DllImport("tioclient.dll", CallingConvention = CallingConvention.Cdecl)]
        public static extern int tiodata_set_as_none(ref TIO_DATA tiodata);

        //void tiodata_set_string_and_size(struct TIO_DATA* tiodata, const void* buffer, unsigned int len)
        [DllImport("tioclient.dll", CallingConvention = CallingConvention.Cdecl)]
        public static extern int tiodata_set_string_and_size(ref TIO_DATA tiodata, IntPtr buffer, int len);

        //void tio_initialize();
        [DllImport("tioclient.dll", CallingConvention = CallingConvention.Cdecl)]
        public static extern int tio_initialize();

        //int tio_connect(const char* host, short port, struct TIO_CONNECTION** connection);
        [DllImport("tioclient.dll", CallingConvention = CallingConvention.Cdecl)]
        public static extern int tio_connect([MarshalAs(UnmanagedType.LPStr)] string host, short port, out IntPtr connectionHandle);

        //int tio_create(struct TIO_CONNECTION* connection, const char* name, const char* type, struct TIO_CONTAINER** container);
        [DllImport("tioclient.dll", CallingConvention = CallingConvention.Cdecl)]
        public static extern int tio_create(
            IntPtr connection,
            [MarshalAs(UnmanagedType.LPStr)] string name,
            [MarshalAs(UnmanagedType.LPStr)] string type,
            out IntPtr nativeContainerHandle);

        //int tio_open(struct TIO_CONNECTION* connection, const char* name, const char* type, struct TIO_CONTAINER** container);
        [DllImport("tioclient.dll", CallingConvention = CallingConvention.Cdecl)]
        public static extern int tio_open(
            IntPtr connection,
            [MarshalAs(UnmanagedType.LPStr)] string name,
            [MarshalAs(UnmanagedType.LPStr)] string type,
            out IntPtr nativeContainerHandle);
        
        //int tio_close(struct TIO_CONTAINER* container);
        [DllImport("tioclient.dll", CallingConvention = CallingConvention.Cdecl)]
        public static extern int tio_close(IntPtr connection);

        // int tio_ping(struct TIO_CONNECTION* connection, char* payload);
        [DllImport("tioclient.dll", CallingConvention=CallingConvention.Cdecl)]
        public static extern int tio_ping(IntPtr connection, [MarshalAs(UnmanagedType.LPStr)] string payload);

        //int tio_container_propget(struct TIO_CONTAINER* container, const struct TIO_DATA* search_key, struct TIO_DATA* value);
        [DllImport("tioclient.dll", CallingConvention = CallingConvention.Cdecl)]
        public static extern int tio_container_propget(
            IntPtr container,
            ref TIO_DATA searchKey,
            out TIO_DATA value);

        //int tio_container_get(struct TIO_CONTAINER* container, const struct TIO_DATA* search_key, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata);
        [DllImport("tioclient.dll", CallingConvention = CallingConvention.Cdecl)]
        public static extern int tio_container_get(
            IntPtr container,
            ref TIO_DATA searchKey,
            out TIO_DATA key,
            out TIO_DATA value,
            out TIO_DATA metadata);

        //int tio_container_set(struct TIO_CONTAINER* container, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata);
        [DllImport("tioclient.dll", CallingConvention = CallingConvention.Cdecl)]
        public static extern int tio_container_set(
            IntPtr container,
            ref TIO_DATA key,
            ref TIO_DATA value,
            ref TIO_DATA metadata);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        public delegate void query_callback_t(
            IntPtr cookie, 
            uint queryid, 
            ref TIO_DATA key,
            ref TIO_DATA value,
            ref TIO_DATA metadata);

        [DllImport("tioclient.dll", CallingConvention = CallingConvention.Cdecl)]
        public static extern int tio_container_query(
            IntPtr container,
            int start, 
            int end, 
            query_callback_t query_callback, 
            IntPtr cookie);

        public static void ThrowOnNativeApiError(int result)
        {
            if(result < 0)
                throw new Exception("tio protocol error");
        }

    }
}
