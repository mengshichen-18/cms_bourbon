# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /root/bourbon/learned-leveldb

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /root/bourbon/build

# Include any dependencies generated for this target.
include CMakeFiles/dbformat_test.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/dbformat_test.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/dbformat_test.dir/flags.make

CMakeFiles/dbformat_test.dir/util/testharness.cc.o: CMakeFiles/dbformat_test.dir/flags.make
CMakeFiles/dbformat_test.dir/util/testharness.cc.o: /root/bourbon/learned-leveldb/util/testharness.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/root/bourbon/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/dbformat_test.dir/util/testharness.cc.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/dbformat_test.dir/util/testharness.cc.o -c /root/bourbon/learned-leveldb/util/testharness.cc

CMakeFiles/dbformat_test.dir/util/testharness.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/dbformat_test.dir/util/testharness.cc.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /root/bourbon/learned-leveldb/util/testharness.cc > CMakeFiles/dbformat_test.dir/util/testharness.cc.i

CMakeFiles/dbformat_test.dir/util/testharness.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/dbformat_test.dir/util/testharness.cc.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /root/bourbon/learned-leveldb/util/testharness.cc -o CMakeFiles/dbformat_test.dir/util/testharness.cc.s

CMakeFiles/dbformat_test.dir/util/testutil.cc.o: CMakeFiles/dbformat_test.dir/flags.make
CMakeFiles/dbformat_test.dir/util/testutil.cc.o: /root/bourbon/learned-leveldb/util/testutil.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/root/bourbon/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Building CXX object CMakeFiles/dbformat_test.dir/util/testutil.cc.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/dbformat_test.dir/util/testutil.cc.o -c /root/bourbon/learned-leveldb/util/testutil.cc

CMakeFiles/dbformat_test.dir/util/testutil.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/dbformat_test.dir/util/testutil.cc.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /root/bourbon/learned-leveldb/util/testutil.cc > CMakeFiles/dbformat_test.dir/util/testutil.cc.i

CMakeFiles/dbformat_test.dir/util/testutil.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/dbformat_test.dir/util/testutil.cc.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /root/bourbon/learned-leveldb/util/testutil.cc -o CMakeFiles/dbformat_test.dir/util/testutil.cc.s

CMakeFiles/dbformat_test.dir/db/dbformat_test.cc.o: CMakeFiles/dbformat_test.dir/flags.make
CMakeFiles/dbformat_test.dir/db/dbformat_test.cc.o: /root/bourbon/learned-leveldb/db/dbformat_test.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/root/bourbon/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Building CXX object CMakeFiles/dbformat_test.dir/db/dbformat_test.cc.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/dbformat_test.dir/db/dbformat_test.cc.o -c /root/bourbon/learned-leveldb/db/dbformat_test.cc

CMakeFiles/dbformat_test.dir/db/dbformat_test.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/dbformat_test.dir/db/dbformat_test.cc.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /root/bourbon/learned-leveldb/db/dbformat_test.cc > CMakeFiles/dbformat_test.dir/db/dbformat_test.cc.i

CMakeFiles/dbformat_test.dir/db/dbformat_test.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/dbformat_test.dir/db/dbformat_test.cc.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /root/bourbon/learned-leveldb/db/dbformat_test.cc -o CMakeFiles/dbformat_test.dir/db/dbformat_test.cc.s

# Object files for target dbformat_test
dbformat_test_OBJECTS = \
"CMakeFiles/dbformat_test.dir/util/testharness.cc.o" \
"CMakeFiles/dbformat_test.dir/util/testutil.cc.o" \
"CMakeFiles/dbformat_test.dir/db/dbformat_test.cc.o"

# External object files for target dbformat_test
dbformat_test_EXTERNAL_OBJECTS =

dbformat_test: CMakeFiles/dbformat_test.dir/util/testharness.cc.o
dbformat_test: CMakeFiles/dbformat_test.dir/util/testutil.cc.o
dbformat_test: CMakeFiles/dbformat_test.dir/db/dbformat_test.cc.o
dbformat_test: CMakeFiles/dbformat_test.dir/build.make
dbformat_test: libleveldb.a
dbformat_test: CMakeFiles/dbformat_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/root/bourbon/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_4) "Linking CXX executable dbformat_test"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/dbformat_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/dbformat_test.dir/build: dbformat_test

.PHONY : CMakeFiles/dbformat_test.dir/build

CMakeFiles/dbformat_test.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/dbformat_test.dir/cmake_clean.cmake
.PHONY : CMakeFiles/dbformat_test.dir/clean

CMakeFiles/dbformat_test.dir/depend:
	cd /root/bourbon/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /root/bourbon/learned-leveldb /root/bourbon/learned-leveldb /root/bourbon/build /root/bourbon/build /root/bourbon/build/CMakeFiles/dbformat_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/dbformat_test.dir/depend

