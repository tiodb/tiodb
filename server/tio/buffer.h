/*
Tio: The Information Overlord
Copyright 2010 Rodrigo Strauss (http://www.1bit.com.br)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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

		Buffer(size_t size)
			: Buffer()
		{
			Alloc(size);
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
