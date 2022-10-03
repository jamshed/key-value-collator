
add_executable(${PROJECT_NAME} main.cpp)

set(INCLUDE_DIR ${CMAKE_SOURCE_DIR}/include)
target_include_directories(${PROJECT_NAME} PUBLIC ${INCLUDE_DIR})

target_compile_options(${PROJECT_NAME} PRIVATE ${WARNING_FLAGS})

set(DEFAULT_INSTALL_DIR ${CMAKE_SOURCE_DIR}/bin)

install(TARGETS ${PROJECT_NAME}
        DESTINATION bin
)
