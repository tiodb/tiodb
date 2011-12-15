using System;
using System.Collections.Generic;
using System.Text;

namespace TioClient
{
    public class Container
    {
        IntPtr _nativeContainerHandle;
        string _name;

        public delegate void QueryCallback(object key, object value, object metadata);

        public string Name { get { return _name; } }

        public Container(IntPtr nativeContainerHandle, string name)
        {
            _nativeContainerHandle = nativeContainerHandle;
            _name = name;
        }

        public void Query(QueryCallback callback)
        {
            NativeImports.tio_container_query(
                _nativeContainerHandle,
                0,
                0,
                delegate(IntPtr cookie,
                uint queryid,
                ref NativeImports.TIO_DATA key,
                ref NativeImports.TIO_DATA value,
                ref NativeImports.TIO_DATA metadata)
                {
                    callback(
                        NativeImports.TioDataConverter.ToObject(key),
                        NativeImports.TioDataConverter.ToObject(value),
                        NativeImports.TioDataConverter.ToObject(metadata));
                },
                IntPtr.Zero);
        }

        public object Get(object searchKey)
        {
            object ret;

            using(NativeImports.TioDataConverter sk = NativeImports.TioDataConverter.FromObject(searchKey),
                  k = new NativeImports.TioDataConverter(),
                  v = new NativeImports.TioDataConverter(),
                  m = new NativeImports.TioDataConverter())
            {
                int result;

                result = NativeImports.tio_container_get(
                    _nativeContainerHandle,
                    ref sk._tiodata,
                    out k._tiodata,
                    out v._tiodata,
                    out m._tiodata);

                ret = v.AsObject();
            }

            return ret;
        }

        public void Set(object key, object value, object metadata = null)
        {
            using(NativeImports.TioDataConverter 
                  k = NativeImports.TioDataConverter.FromObject(key),
                  v = NativeImports.TioDataConverter.FromObject(value),
                  m = NativeImports.TioDataConverter.FromObject(metadata))
            {
                int result;

                result = NativeImports.tio_container_set(
                    _nativeContainerHandle,
                    ref k._tiodata,
                    ref v._tiodata,
                    ref m._tiodata);

                NativeImports.ThrowOnNativeApiError(result);
            }
        }

        public string GetProperty(string key)
        {
            int result;
            using(NativeImports.TioDataConverter 
                k = NativeImports.TioDataConverter.FromObject(key),
                v = new NativeImports.TioDataConverter())
            {
                result = NativeImports.tio_container_propget(
                    _nativeContainerHandle,
                    ref k._tiodata,
                    out v._tiodata);

                NativeImports.ThrowOnNativeApiError(result);

                return v.ToString();
            }
        }

    }
}
