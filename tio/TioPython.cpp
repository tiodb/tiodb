

#include "pch.h"
#include "TioPython.h"
#include "ContainerManager.h"

namespace tio
{
	using std::vector;
	using std::string;
	using std::cout;
	using std::endl;
	using boost::shared_ptr;
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

#define INITIALIZE_PYTHON_WRAPPER(T) python::object T::pythonWrapperT;

	TioData PythonObjectToTioData(const python::object obj)
	{
		if(PyString_Check(obj.ptr()))
			return TioData(python::extract<string>(obj));
		else if(PyInt_Check(obj.ptr()))
			return TioData(python::extract<int>(obj));
		else if(PyFloat_Check(obj.ptr()))
			return TioData(python::extract<float>(obj));

		throw std::runtime_error("invalid python type");
	}

	python::object TioDataToPythonObject(const TioData v)
	{
		switch(v.GetDataType())
		{
		case TioData::Sz:
			return python::object(v.AsSz());
		case TioData::Int:
			return python::object(v.AsInt());
		case TioData::Double:
			return python::object(v.AsDouble());
		}

		return python::object();
	}

	class TioContainerWrapper : public PythonWrapperImpl< shared_ptr<ITioContainer>, TioContainerWrapper>
	{
	public:

		static python::object InitPythonType()
		{
			return
				python::class_<TioContainerWrapper>("Container")
					.def("push_back", &TioContainerWrapper::push_back)
					.def("get_count", &TioContainerWrapper::get_count)
					.def("__len__", &TioContainerWrapper::get_count)
					.add_property("name", &TioContainerWrapper::name);
		}

		int get_count()
		{
			return wrapped_->GetRecordCount();
		}


		
		void push_back(python::object key, python::object value, python::object metadata)
		{
			wrapped_->PushBack(
				PythonObjectToTioData(key),
				PythonObjectToTioData(value),
				PythonObjectToTioData(metadata));
		}

		string name()
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
						.def("CreateContainer", &TioContainerManagerWrapper::CreateContainer)
						.def("OpenContainer", &TioContainerManagerWrapper::OpenContainer1)
						.def("OpenContainer", &TioContainerManagerWrapper::OpenContainer2)
						.def("DeleteContainer", &TioContainerManagerWrapper::DeleteContainer)
						.def("Exists", &TioContainerManagerWrapper::Exists);

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

	INITIALIZE_PYTHON_WRAPPER(TioContainerManagerWrapper);
	INITIALIZE_PYTHON_WRAPPER(TioContainerWrapper);

	void InitializePythonSupport(const char* programName, ContainerManager* containerManager)
	{
		Py_SetProgramName(const_cast<char*>(programName));
		Py_Initialize();
		PyRun_SimpleString("print 'Python support initialized:'");

		/*
		//
		// Show Python version
		//
		python::object sys = boost::python::import("sys");
		string version = boost::python::extract<std::string>(sys.attr("version"));
		cout << version << endl;

		python::object main_module((
			python::handle<>(python::borrowed(PyImport_AddModule("__main__")))));

		python::object main_namespace = main_module.attr("__dict__");
		*/

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

	void LoadPythonPlugins(const vector<string>& plugins)
	{
		boost::python::object imp = boost::python::import("imp");
		boost::python::object load_source = imp.attr("load_source");
	
		BOOST_FOREACH(const string& pluginPath, plugins)
		{
			try
			{
				boost::python::object pluginModule = load_source(boost::filesystem::path(pluginPath).stem(), pluginPath);
				pluginModule.attr("TioPluginMain")(g_pythonContainerManager);
			}
			catch(boost::python::error_already_set&)
			{
				PyErr_Print();
				throw std::runtime_error(string("Error loading python plugin ") + pluginPath);
			}
		}
	}

}

