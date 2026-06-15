cmake_minimum_required(VERSION 3.28 FATAL_ERROR)

project(NutBlast
    VERSION 0.1.0
    LANGUAGES C CXX)

option(NUTBLAST_BUILD_TEST "Build a NutBlast test binary?" OFF)
option(NUTBLAST_DEV_LOCALHOST "Connect to ws://localhost:36900 by default?" OFF)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

set(BUILD_SHARED_LIBS OFF)
set(BUILD_STATIC_LIBS ON)

set(CMAKE_COMPILE_WARNING_AS_ERROR OFF) # libjuice won't build otherwise, damn it

if(NUTBLAST_BUILD_TEST AND CMAKE_SYSTEM_NAME MATCHES "Emscripten")
    set(CMAKE_EXECUTABLE_SUFFIX ".html")
    set(PLATFORM Web CACHE INTERNAL "")
endif()

include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

add_library(NutBlast
    ${CMAKE_CURRENT_SOURCE_DIR}/NutBlast.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/NutBlast.h)
target_include_directories(NutBlast
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(NutBlast PROPERTIES LINKER_LANGUAGE CXX)

if(NUTBLAST_DEV_LOCALHOST)
    target_compile_definitions(NutBlast PUBLIC NUTBLAST_DEV_LOCALHOST)
endif()

FetchContent_Declare(json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.12.0
    OVERRIDE_FIND_PACKAGE
    EXCLUDE_FROM_ALL)

list(APPEND LIBS nlohmann_json::nlohmann_json)
list(APPEND DEPS json)

set(USE_SYSTEM_JSON ON CACHE INTERNAL "")
set(NO_MEDIA ON CACHE INTERNAL "")
set(NO_EXAMPLES ON CACHE INTERNAL "")
set(NO_TESTS ON CACHE INTERNAL "")
set(USE_MBEDTLS ON CACHE INTERNAL "")
set(ENABLE_PROGRAMS OFF CACHE INTERNAL "")
set(ENABLE_TESTING OFF CACHE INTERNAL "")
set(INSTALL_DEPS_LIBS OFF CACHE INTERNAL "")
set(MBEDTLS_USER_CONFIG_FILE ${CMAKE_CURRENT_SOURCE_DIR}/mbedtls.h CACHE INTERNAL "")

if(CMAKE_SYSTEM_NAME MATCHES "Emscripten")
    FetchContent_Declare(datachannel-wasm
        GIT_REPOSITORY https://github.com/paullouisageneau/datachannel-wasm.git
        GIT_TAG v0.4.0
        EXCLUDE_FROM_ALL)

    list(APPEND LIBS datachannel-wasm)
    list(APPEND DEPS datachannel-wasm)

    target_link_options(NutBlast PUBLIC -sASYNCIFY -sEXPORTED_RUNTIME_METHODS=HEAPU32,HEAPU8)
    target_compile_options(NutBlast PUBLIC -O3) # the emscripten manual recommends this
else()
    FetchContent_Declare(MbedTLS
        GIT_REPOSITORY https://github.com/Mbed-TLS/mbedtls.git
        GIT_TAG v3.6.6
        OVERRIDE_FIND_PACKAGE
        EXCLUDE_FROM_ALL)

    FetchContent_Declare(datachannel
        GIT_REPOSITORY https://github.com/Schwungus/libdatachannel.git
        GIT_TAG master
        EXCLUDE_FROM_ALL)

    list(APPEND LIBS datachannel-static)
    list(APPEND DEPS MbedTLS datachannel)
endif()

FetchContent_MakeAvailable(${DEPS})
target_link_libraries(NutBlast PUBLIC ${LIBS})

if(NUTBLAST_BUILD_TEST)
    FetchContent_Declare(raylib
        GIT_REPOSITORY https://github.com/raysan5/raylib.git
        GIT_TAG 5.5
        EXCLUDE_FROM_ALL)

    FetchContent_Declare(S_tructures
        GIT_REPOSITORY https://github.com/Schwungus/S_tructures.git
        GIT_TAG 484e78265ec3e58cf57bfe7aa09b53d48b60300f
        EXCLUDE_FROM_ALL)

    FetchContent_MakeAvailable(raylib S_tructures)

    add_executable(NutBlast-test ${CMAKE_CURRENT_SOURCE_DIR}/NutBlast-test.c)
    target_link_libraries(NutBlast-test PRIVATE NutBlast raylib S_tructures)

    add_executable(NutBlast-lobbyist ${CMAKE_CURRENT_SOURCE_DIR}/NutBlast-lobbyist.c)
    target_link_libraries(NutBlast-lobbyist PRIVATE NutBlast)
endif()
