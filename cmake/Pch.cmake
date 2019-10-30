
function (add_pch target pchHeaderFile pchSourceFile sourceFiles pchExcludeFiles)
	# MSVC

	# /Yc creates a precompiled header file
	# /Yu specifies project to use existing precompiled header file
	# /Y- ignoreprecompiled header options
	# /Fp specifies precompiled header binary file name
	# /FI forces inclusion of file
	# /TC treat all files named on the command line as C source files
	# /TP treat all files named on the command line as C++ source files
	# /Zs syntax check only
	# /Zm precompiled header memory allocation scaling factor
	# /Zi generate complete debugging information
	if(${ARGC} GREATER 5)
		message(FATAL_ERROR "You probably are passing the content of source files and not variable.")
	endif()

	if(MSVC)
		set_target_properties(tio PROPERTIES COMPILE_FLAGS "/Yu${pchHeaderFile}")
		set_source_files_properties(${pchSourceFile} PROPERTIES COMPILE_FLAGS "/Yc${pchHeaderFile}")

		foreach(item ${SOURCE_FILES})
			if(${item} IN_LIST ${pchExcludeFiles})
				message("Removing pch for file ${item}")
				set_source_files_properties(${item} PROPERTIES COMPILE_FLAGS "/Y-")
			endif()

		endforeach()

	endif(MSVC)

	# TODO: Make compatible with others compilers

endfunction()
