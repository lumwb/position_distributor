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
    src/shared_memory/sbe_encoding.cpp
    src/shared_memory/shared_memory_manager.cpp
    src/shared_memory/position_media_driver.cpp
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

# Media Driver executable
add_executable(media_driver
    src/shared_memory/media_driver_main.cpp
)
target_link_libraries(media_driver position_shm Threads::Threads)

# Example executables
add_executable(exchange_client_example
    examples/exchange_client_example.cpp
)
target_link_libraries(exchange_client_example position_shm Threads::Threads)

add_executable(simple_publisher_example
    examples/simple_publisher_example.cpp
)
target_link_libraries(simple_publisher_example position_shm Threads::Threads)

add_executable(simple_subscriber_example
    examples/simple_subscriber_example.cpp
)
target_link_libraries(simple_subscriber_example position_shm Threads::Threads)

add_executable(demo_test
    examples/demo_test.cpp
)
target_link_libraries(demo_test position_shm Threads::Threads)

add_executable(two_client_demo
    examples/two_client_demo.cpp
)
target_link_libraries(two_client_demo position_shm Threads::Threads)

add_executable(test_publisher
    examples/test_publisher.cpp
)
target_link_libraries(test_publisher position_shm Threads::Threads)

add_executable(test_subscriber
    examples/test_subscriber.cpp
)
target_link_libraries(test_subscriber position_shm Threads::Threads)

add_executable(multi_subscriber_demo
    examples/multi_subscriber_demo.cpp
)
target_link_libraries(multi_subscriber_demo position_shm Threads::Threads)

# Installation
install(TARGETS media_driver exchange_client_example simple_publisher_example simple_subscriber_example DESTINATION bin)
install(DIRECTORY include/ DESTINATION include)
