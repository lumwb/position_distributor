cmake_minimum_required(VERSION 3.16)
project(PositionDistributor VERSION 2.0.0 LANGUAGES CXX)

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

# Find required packages
find_package(Threads REQUIRED)

# Common source files
set(COMMON_SOURCES
    src/common/position.cpp
    src/common/logger.cpp
)

# Shared memory implementation sources
set(SHARED_MEMORY_SOURCES
    src/shared_memory/position_encoding.cpp
    src/shared_memory/topic_system.cpp
)

set(PUBLISHER_SOURCES
    src/publisher/position_publisher.cpp
)

set(SUBSCRIBER_SOURCES
    src/subscriber/position_subscriber.cpp
)

set(CLIENT_SOURCES
    src/client/position_client.cpp
)

# Create shared memory library
add_library(position_shm STATIC
    ${COMMON_SOURCES}
    ${SHARED_MEMORY_SOURCES}
    ${PUBLISHER_SOURCES}
    ${SUBSCRIBER_SOURCES}
    ${CLIENT_SOURCES}
)

target_link_libraries(position_shm Threads::Threads)

# Example executables
add_executable(exchange_client_example
    examples/exchange_client_example.cpp
)
target_link_libraries(exchange_client_example position_shm Threads::Threads)

# Installation
install(TARGETS exchange_client_example DESTINATION bin)
install(DIRECTORY include/ DESTINATION include)
