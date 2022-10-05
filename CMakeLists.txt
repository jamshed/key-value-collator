
cmake_minimum_required(VERSION 3.14)

set(PROJECT_NAME key_value_collator)
project(${PROJECT_NAME}
    VERSION 0.1.0   # https://semver.org/
    LANGUAGES CXX
)


set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(WARNING_FLAGS -Wall -Wextra)    # https://gcc.gnu.org/onlinedocs/gcc/Warning-Options.html


include(FindThreads)
if(NOT Threads_FOUND)
    message(FATAL_ERROR "A supported threads library is required. Aborting.")
endif()


if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()


add_subdirectory(src)
