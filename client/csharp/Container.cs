using System;
using System.Collections.Generic;
using System.Text;

namespace TioClient
{
    public class Container
    {
        IntPtr _nativeContainerHandle;
        string _name;

        public enum EventCode
        {
            Ping = 0x10,
            Open = 0x11,
            Create = 0x12,
            Close = 0x13,
            Set = 0x14,
            Insert = 0x15,
            Delete = 0x16,
            PushBack = 0x17,
            PushFront = 0x18,
            PopBack = 0x19,
            PopFront = 0x1a,
            Clear = 0x1b,
            Count = 0x1c,
            Get = 0x1d,
            Subscribe = 0x1e,
            Unsubscribe = 0x1f,
            Query = 0x20,
            WaitAndPopNext = 0x21,
            WaitAndPopKey = 0x22
        }

        public delegate void QueryCallback(object key, object value, object metadata);
        public delegate void EventCallback(EventCode eventCode, object key, object value, object metadata);

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
                null,
                delegate(int result,
                IntPtr handle,
                IntPtr cookie,
                uint queryid,
                string containerName,
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



        public void Subscribe(EventCallback callback)
        {
            NativeImports.TIO_DATA start = new NativeImports.TIO_DATA();// = NativeImports.TioDataConverter.FromObject(null);            

            NativeImports.tio_container_subscribe(
                _nativeContainerHandle,
               ref start,
                delegate(int result,
                    IntPtr cookie,
                    IntPtr handle,
                    uint eventCode,
                    ref NativeImports.TIO_DATA key,
                    ref NativeImports.TIO_DATA value,
                    ref NativeImports.TIO_DATA metadata)
                    {
                        EventCode convertedEventCode = (EventCode)eventCode;

                        callback(
                            convertedEventCode,
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


        public int Count
        {
            get
            {
                int result;
                int count;

                result = NativeImports.tio_container_get_count(
                    _nativeContainerHandle,
                    out count);

                return count;
            }
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
