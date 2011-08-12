using System;
using System.Collections.Generic;
using System.Text;

namespace TioClient
{
    class Container
    {
        IntPtr _nativeContainerHandle;


        public Container(IntPtr nativeContainerHandle)
        {
            _nativeContainerHandle = nativeContainerHandle;
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
    }
}
