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

//
// Python plugin support. Need Python and Boost Python
//
#ifndef TIO_PYTHON_PLUGIN_SUPPORT
#define TIO_PYTHON_PLUGIN_SUPPORT 0
#endif


#define _CRT_SECURE_NO_DEPRECATE
#define _SCL_SECURE_NO_WARNINGS
#define _WINSOCK_DEPRECATED_NO_WARNINGS
//#define _SECURE_SCL 0

#define _WIN32_WINNT 0x0500
#define WIN32_LEAN_AND_MEAN

#pragma warning(disable : 4127) // conditional expression is constant
#pragma warning(disable : 4512) // assignment operator could not be generated
#pragma warning(disable : 4100) // unreferenced local parameter
#pragma warning(disable : 4121) //: 'boost::python::detail::aligned_storage<size>' : alignment of a member was sensitive to packing


//
// lexical_cast is triggering this warning every time, it's pissing me off
//
#pragma warning(disable: 4267)

//
// "non standard extension" every time, but GCC is happy as well
//
#pragma warning(disable: 4238) 


#include <stdio.h>
#ifdef _WIN32
  #include <tchar.h>
#else
  #define min(x,y) (x<y?x:y)
  #include <sys/types.h>
  #include <sys/stat.h>
  #include <unistd.h>
//  #include <google/profiler.h>
#endif

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/array.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/shared_array.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/ptr_container/ptr_map.hpp>
#include <boost/ptr_container/ptr_set.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

#include <boost/program_options.hpp>

#include <boost/tuple/tuple.hpp>

#include <boost/foreach.hpp>

#include <boost/typeof/std/utility.hpp>
#include <boost/typeof/std/vector.hpp>

#include <boost/regex.hpp>

#if TIO_PYTHON_PLUGIN_SUPPORT
#include <boost/python.hpp>
#endif

#include <iostream>
#include <iomanip>
#include <sstream>
#include <numeric>
#include <queue>
#include <deque>
#include <limits>

//
// macros are evil, you know?
//
#if defined(max) 
#undef max
#endif

//
// berkeley db
//
//#include <db_cxx.h>

#ifndef INTERFACE
#define INTERFACE struct
#endif

#ifndef PURE
#define PURE =0
#endif

#define ASSERT BOOST_ASSERT

template<typename T, typename V>
bool found(T& container, V& value)
{
	using boost::const_end;
	using boost::const_begin;
	return std::find(const_begin(container), const_end(container), value) != const_end(container);
}

template<typename T, typename V>
bool key_found(T& container, V& value)
{
	return container.find(value) != container.end();
}

#ifndef min
template<typename T1, typename T2> inline T1 min (T1 l, T2 r) { return l < r ? l : r; }
#endif

//
// fake mutex, so we can switch mutithread on and off
//
namespace tio
{
	class recursive_mutex
	{
	public:
		class scoped_lock
		{
		public:
			scoped_lock(recursive_mutex& m){}
		};
	};
}


