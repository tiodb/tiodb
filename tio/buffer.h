#pragma once

#ifndef _WIN32
#define min(x,y) (x<y?x:y)
#endif

namespace tio
{
	class Buffer : boost::noncopyable
	{
		void* buffer_;
		size_t size_;

	public:
		Buffer(void* data, size_t size)
			: buffer_(NULL), size_(0)
		{
			Set(data, size);
		}

		Buffer()
			: buffer_(NULL), size_(0)
		{}

		~Buffer()
		{
			Free();
		}

		void Free()
		{
			delete (unsigned char*)buffer_;
			buffer_ = 0;
			size_ = 0;
		}

		void Alloc(size_t size)
		{
			Free();
			buffer_ = new unsigned char[size];
			size_ = size;
		}

		void* GetRawBuffer()
		{
			return buffer_;
		}

		void GetCopy(void* data, size_t size)
		{
			memcpy(data, buffer_, min(size, size_));
		}

		size_t GetSize() const
		{
			return size_;
		}

		void EnsureMinSize(size_t size)
		{
			if(size_ < size)
				Alloc(size);
		}

		void Set(void* data, size_t size)
		{
			Alloc(size);

			BOOST_ASSERT(!!data == !!size);

			if(!data)
				return;

			memcpy(buffer_, data, size);
		}
	};
}
