#include "..\tioclient\tioclient.hpp"

#ifdef TIOTESTPLUGIN_EXPORTS
#define TIOTESTPLUGIN_API __declspec(dllexport)
#else
#define TIOTESTPLUGIN_API __declspec(dllimport)
#endif

void TIOTESTPLUGIN_API tio_plugin_start(void* container_manager);