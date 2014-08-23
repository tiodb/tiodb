#include "pch.h"

#if TIO_PYTHON_PLUGIN_SUPPORT
#include "TioPython.h"
#include "ContainerManager.h"

namespace tio
{
	using std::vector;
	using std::string;
	using std::cout;
	using std::endl;
	using std::shared_ptr;
	namespace python = boost::python;

	python::object g_pythonContainerManager;

	template<typename T, typename TDerived>
	class PythonWrapperImpl
	{
		typedef PythonWrapperImpl<T, TDerived> this_type;

		void SetWrapped(T wrapped)
		{
			wrapped_ = wrapped;
		}

		static void InitWrapperT()
		{
			if(pythonWrapperT.is_none())
				pythonWrapperT = TDerived::InitPythonType();
		}

	protected:
		T wrapped_;
		static python::object pythonWrapperT;
	public:

		static python::object CreateWrapper(T wrapped)
		{
			InitWrapperT();

			//
			// create the wrapper as Python object
			//
			python::object pythonWrapper = pythonWrapperT();

			//
			// extract the C++ object
			//
			TDerived& wrapper = python::extract<TDerived&>(pythonWrapper);

			//
			// set the wrapped C++ object
			//
			wrapper.SetWrapped(wrapped);
			
			return pythonWrapper;
		}
	};

	template<typename T, typename TDerived> python::object PythonWrapperImpl<T, TDerived>::pythonWrapperT;

	
//#define INITIALIZE_PYTHON_WRAPPER(T) python::object T::pythonWrapperT;

	TioData PythonObjectToTioData(const python::object& obj)
	{
		if(PyString_Check(obj.ptr()))
			return TioData(python::extract<string>(obj));
		else if(PyInt_Check(obj.ptr()))
			return TioData(python::extract<int>(obj));
		else if(PyFloat_Check(obj.ptr()))
			return TioData(python::extract<float>(obj));

		throw std::runtime_error("invalid python type");
	}

	python::object TioDataToPythonObject(const TioData& v)
	{
		switch(v.GetDataType())
		{
		case TioData::String:
			return python::str(v.AsSz(), v.GetSize());
		case TioData::Int:
			return python::object(v.AsInt());
		case TioData::Double:
			return python::object(v.AsDouble());
		}

		return python::object();
	}

	class TioResultSetWrapper : public PythonWrapperImpl<shared_ptr<ITioResultSet> , TioResultSetWrapper>
	{
	public:
		static python::object InitPythonType()
		{
				return 
					python::class_<TioResultSetWrapper>("ResultSet")
						.def("get", &TioResultSetWrapper::GetRecord)
						.def("next", &TioResultSetWrapper::MoveNext)
						.def("previous", &TioResultSetWrapper::MovePrevious)
						.def("at_begin", &TioResultSetWrapper::AtBegin)
						.def("at_end", &TioResultSetWrapper::AtEnd)
						.def("count", &TioResultSetWrapper::RecordCount);
		}

		void MoveNext()
		{ 
			wrapped_->MoveNext(); 
		}
		void MovePrevious() { wrapped_->MovePrevious(); }
		bool AtBegin() { return wrapped_->AtBegin(); }
		bool AtEnd() { return wrapped_->AtEnd(); }
		unsigned int RecordCount() { return wrapped_->RecordCount();}

		python::object Source() 
		{ 
			return TioDataToPythonObject(wrapped_->Source());
		}

		python::object GetRecord()
		{
			TioData key, value, metadata;
			
			bool exists = wrapped_->GetRecord(&key, &value, &metadata);

			if(!exists)
				throw std::runtime_error("no more records");

			return python::make_tuple(
				TioDataToPythonObject(key),
				TioDataToPythonObject(value),
				TioDataToPythonObject(metadata));
		}
	};

	class TioContainerWrapper : public PythonWrapperImpl< shared_ptr<ITioContainer>, TioContainerWrapper>
	{
	public:

		static python::object InitPythonType()
		{
			using python::arg;

			python::object containerType = 
				python::class_<TioContainerWrapper>("Container")
					.def("push_back", &TioContainerWrapper::PushBack1, (arg("value")))
					.def("push_back", &TioContainerWrapper::PushBack2, (arg("value"), arg("metadata")))
					.def("append", &TioContainerWrapper::PushBack1, (arg("value")))
					.def("append", &TioContainerWrapper::PushBack2, (arg("value"), arg("metadata")))
					.def("pop_back", &TioContainerWrapper::PopBack)
					.def("push_front", &TioContainerWrapper::PushFront)
					.def("pop_front", &TioContainerWrapper::PopFront)
					.def("insert", &TioContainerWrapper::Insert)
					.def("insert", &TioContainerWrapper::Insert1)
					.def("set", &TioContainerWrapper::Set3, (arg("key"), arg("value"), arg("metadata")))
					.def("set", &TioContainerWrapper::Set2, (arg("key"), arg("value")))
					.def("get", &TioContainerWrapper::GetRecord, (arg("key")))
					.def("delete", &TioContainerWrapper::Delete)
					.def("clear", &TioContainerWrapper::Clear)
					.def("propget", &TioContainerWrapper::SetProperty)
					.def("propset", &TioContainerWrapper::GetProperty)
					.def("subscribe", &TioContainerWrapper::Subscribe)
					.def("subscribe", &TioContainerWrapper::Subscribe1)
					.def("unsubscribe", &TioContainerWrapper::Unsubscribe)

					.def("values", &TioContainerWrapper::Values)
					.def("keys", &TioContainerWrapper::Keys)
					.def("query", &TioContainerWrapper::Values)
					.def("query_with_key_and_metadata", &TioContainerWrapper::Query)
					.def("query_with_key_and_metadata", &TioContainerWrapper::Query0)

					.def("__len__", &TioContainerWrapper::GetRecordCount)
					.def("get_count", &TioContainerWrapper::GetRecordCount)
					.def("__getitem__", &TioContainerWrapper::GetRecordValue)
					.def("__setitem__", &TioContainerWrapper::Set2)
					.def("__delitem__",  &TioContainerWrapper::Delete1)
					.add_property("name", &TioContainerWrapper::GetName);

			return containerType;
		}

		static void PythonCallbackBridge(python::object container, python::object callback, const string& eventFilter, const string& eventName, 
			const TioData& key, const TioData& value, const TioData& metadata)
		{
			if(eventFilter.empty() == false && eventFilter != eventName)
				return;

			callback(
				container,
				eventName,
				TioDataToPythonObject(key),
				TioDataToPythonObject(value),
				TioDataToPythonObject(metadata));
		}

		int Subscribe(python::object callback, const string& eventFilter, python::object start)
		{
			return wrapped_->Subscribe(
				boost::bind(&TioContainerWrapper::PythonCallbackBridge, python::object(this), callback, eventFilter == "*" ? string() : eventFilter, _1, _2, _3, _4),
				"0");
		}

		int Subscribe1(python::object callback)
		{
			return Subscribe(callback, string(), python::object());
		}

		void Unsubscribe(unsigned int cookie)
		{
			wrapped_->Unsubscribe(cookie);
		}

		void Clear()
		{
			wrapped_->Clear();
		}

		string GetProperty(const string& key)
		{
			return wrapped_->GetProperty(key);
		}

		void SetProperty(const string& key, const string& value)
		{
			return wrapped_->SetProperty(key, value);
		}

		int GetRecordCount()
		{
			return wrapped_->GetRecordCount();
		}
		/*
		def query(self, startOffset=None, endOffset=None):
        # will return only the values
        return [x[1] for x in self.query_with_key_and_metadata(startOffset, endOffset)]

		def query_with_key_and_metadata(self, startOffset=None, endOffset=None):
        return self.manager.Query(self.handle, startOffset, endOffset)
		*/

		python::object Values()
		{
			shared_ptr<ITioResultSet> resultSet = wrapped_->Query(0, 0, TIONULL);

			TioData value;
			python::list ret;

			while(resultSet->GetRecord(NULL, &value, NULL))
			{
				ret.append(TioDataToPythonObject(value));
				resultSet->MoveNext();
			}

			return ret;
		}

		python::object Keys()
		{
			shared_ptr<ITioResultSet> resultSet = wrapped_->Query(0, 0, TIONULL);

			TioData key;
			python::list ret;

			while(resultSet->GetRecord(&key, NULL, NULL))
			{
				ret.append(TioDataToPythonObject(key));
				resultSet->MoveNext();
			}

			return ret;
		}

		python::object Query0()
		{
			return Query(0, 0);
		}

		python::object Query(unsigned start, unsigned end)
		{
			shared_ptr<ITioResultSet> resultSet = wrapped_->Query(start, end, TIONULL);

			return TioResultSetWrapper::CreateWrapper(resultSet);
		}

		python::object GetRecord(python::object searchKey)
		{
			TioData key, value, metadata;
			
			wrapped_->GetRecord(PythonObjectToTioData(searchKey), &key, &value, &metadata);

			return python::make_tuple(
				TioDataToPythonObject(key),
				TioDataToPythonObject(value),
				TioDataToPythonObject(metadata));
		}

		python::object GetRecordValue(python::object searchKey)
		{
			TioData key, value;
			
			wrapped_->GetRecord(PythonObjectToTioData(searchKey), &key, &value, NULL);

			return TioDataToPythonObject(value);
		}

		python::object PopBack(python::object searchKey)
		{
			TioData key, value, metadata;
			
			wrapped_->PopBack(&key, &value, &metadata);

			return python::make_tuple(
				TioDataToPythonObject(key),
				TioDataToPythonObject(value),
				TioDataToPythonObject(metadata));
		}

		python::object PopFront(python::object searchKey)
		{
			TioData key, value, metadata;
			
			wrapped_->PopFront(&key, &value, &metadata);

			return python::make_tuple(
				TioDataToPythonObject(key),
				TioDataToPythonObject(value),
				TioDataToPythonObject(metadata));
		}

		void PushFront(python::object key, python::object value, python::object metadata)
		{
			wrapped_->PushFront(
				PythonObjectToTioData(key),
				PythonObjectToTioData(value),
				PythonObjectToTioData(metadata));
		}

		void PushBack2(python::object value, python::object metadata)
		{
			wrapped_->PushBack(
				TIONULL,
				PythonObjectToTioData(value),
				PythonObjectToTioData(metadata));
		}

		void PushBack1(python::object value)
		{
			wrapped_->PushBack(TIONULL, PythonObjectToTioData(value));
		}

		void Insert(python::object key, python::object value, python::object metadata)
		{
			wrapped_->Insert(
				PythonObjectToTioData(key),
				PythonObjectToTioData(value),
				PythonObjectToTioData(metadata));
		}

		void Insert1(python::object key, python::object value)
		{
			wrapped_->Insert(
				PythonObjectToTioData(key),
				PythonObjectToTioData(value));
		}

		void Set3(python::object key, python::object value, python::object metadata)
		{
			wrapped_->Set(
				PythonObjectToTioData(key),
				PythonObjectToTioData(value),
				PythonObjectToTioData(metadata));
		}

		void Set2(python::object key, python::object value)
		{
			wrapped_->Set(
				PythonObjectToTioData(key),
				PythonObjectToTioData(value),
				TIONULL);
		}

		void Delete1(python::object key)
		{
			wrapped_->Delete(PythonObjectToTioData(key));
		}

		void Delete(python::object key, python::object value, python::object metadata)
		{
			wrapped_->Delete(
				PythonObjectToTioData(key),
				PythonObjectToTioData(value),
				PythonObjectToTioData(metadata));
		}

		string GetName()
		{
			return wrapped_->GetName();
		}
	};

	class TioContainerManagerWrapper : public PythonWrapperImpl<ContainerManager* , TioContainerManagerWrapper>
	{
	public:
		static python::object InitPythonType()
		{
				return 
					python::class_<TioContainerManagerWrapper>("ContainerManager")
						.def("create", &TioContainerManagerWrapper::CreateContainer)
						.def("open", &TioContainerManagerWrapper::OpenContainer1)
						.def("open", &TioContainerManagerWrapper::OpenContainer2)
						.def("delete", &TioContainerManagerWrapper::DeleteContainer)
						.def("exists", &TioContainerManagerWrapper::Exists);

		}

		python::object CreateContainer(string name, string type)
		{
			return TioContainerWrapper::CreateWrapper(
				wrapped_->CreateContainer(type, name));
		}

		python::object OpenContainer2(string name, string type)
		{
			return TioContainerWrapper::CreateWrapper(
				wrapped_->OpenContainer(type, name));
		}

		python::object OpenContainer1(string name)
		{
			return TioContainerWrapper::CreateWrapper(
				wrapped_->OpenContainer(string(), name));
		}

		void DeleteContainer(const string& name, const string& type)
		{
			wrapped_->DeleteContainer(type, name);
		}

		bool Exists(string name, string type)
		{
			return wrapped_->Exists(type, name);
		}
	};

//	INITIALIZE_PYTHON_WRAPPER(TioContainerManagerWrapper);
//	INITIALIZE_PYTHON_WRAPPER(TioContainerWrapper);

	void InitializePythonSupport(const char* programName, ContainerManager* containerManager)
	{
		Py_SetProgramName(const_cast<char*>(programName));
		Py_Initialize();
		PyRun_SimpleString("print 'Python support initialized:'");

		try
		{
			g_pythonContainerManager = TioContainerManagerWrapper::CreateWrapper(containerManager);
		}
		catch(boost::python::error_already_set&)
		{
			PyErr_Print();
			throw std::runtime_error("Error loading python infrastructure ");
		}
	}

	void LoadPythonPlugins(const vector<string>& plugins, const std::map<std::string, std::string>& parameters)
	{
		boost::python::object imp = boost::python::import("imp");
		boost::python::object load_source = imp.attr("load_source");
		boost::python::dict pythonParameters;

		//
		// Convert map to python's dict
		//
		for(std::map<std::string, std::string>::const_iterator i = parameters.begin() ; i != parameters.end() ; ++i)
			pythonParameters[i->first] = i->second;
	
		BOOST_FOREACH(const string& pluginPath, plugins)
		{
			try
			{
				boost::python::object pluginModule = 
					load_source(
						boost::filesystem::path(pluginPath).stem()
#if(BOOST_VERSION >= 104700)
							.string()
#endif
					, pluginPath);
				pluginModule.attr("TioPluginMain")(g_pythonContainerManager, pythonParameters);
			}
			catch(boost::python::error_already_set&)
			{
				PyErr_Print();
				throw std::runtime_error(string("Error loading python plugin ") + pluginPath);
			}
		}
	}

}

#endif //TIO_PYTHON_PLUGIN_SUPPORT