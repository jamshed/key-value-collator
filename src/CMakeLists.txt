
set(EXECUTABLE main)
add_executable(${EXECUTABLE} main.cpp)

set(INCLUDE_DIR ${CMAKE_SOURCE_DIR}/include)
target_include_directories(${EXECUTABLE} PUBLIC ${INCLUDE_DIR})

target_compile_options(${EXECUTABLE} PRIVATE ${WARNING_FLAGS})

set(DEFAULT_INSTALL_DIR ${CMAKE_SOURCE_DIR}/bin)

install(TARGETS ${EXECUTABLE}
        DESTINATION bin
)
