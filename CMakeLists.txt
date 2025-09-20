cmake_minimum_required(VERSION 3.16)
project(PositionDistributor VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set build type if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# Include directories
include_directories(include)

# Source files
set(COMMON_SOURCES
    src/common/position.cpp
    src/common/logger.cpp
    src/common/simple_network.cpp
)

set(SERVER_SOURCES
    src/server/simple_position_server.cpp
    src/server/simple_position_server_main.cpp
    ${COMMON_SOURCES}
)

set(CLIENT_SOURCES
    src/client/simple_position_client.cpp
    src/client/simple_position_client_main.cpp
    ${COMMON_SOURCES}
)

# Create executables
add_executable(position_server ${SERVER_SOURCES})
add_executable(position_client ${CLIENT_SOURCES})

# Link libraries
target_link_libraries(position_server pthread)
target_link_libraries(position_client pthread)

# Create example executable
add_executable(simple_test 
    examples/simple_test.cpp
    src/common/position.cpp
    src/common/logger.cpp
    src/common/simple_network.cpp
    src/server/simple_position_server.cpp
    src/client/simple_position_client.cpp
)

target_link_libraries(simple_test pthread)

# Installation
install(TARGETS position_server position_client DESTINATION bin)
