cmake_minimum_required(VERSION 3.28 FATAL_ERROR)

project(NutBlast
    VERSION 0.1.0
    LANGUAGES C CXX)

option(NUTBLAST_BUILD_TEST "Build a NutBlast test binary?" OFF)

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

FetchContent_Declare(json
    URL https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz)
list(APPEND LIBS nlohmann_json::nlohmann_json)

set(NO_MEDIA ON CACHE INTERNAL "")
set(NO_EXAMPLES ON CACHE INTERNAL "")
set(NO_TESTS ON CACHE INTERNAL "")

if(CMAKE_SYSTEM_NAME MATCHES "Emscripten")
    FetchContent_Declare(datachannel
        GIT_REPOSITORY https://github.com/paullouisageneau/datachannel-wasm.git
        GIT_TAG v0.4.0
        EXCLUDE_FROM_ALL)
    list(APPEND LIBS datachannel-wasm)

    target_link_options(NutBlast PUBLIC -sASYNCIFY -sEXPORTED_RUNTIME_METHODS=HEAPU32,HEAPU8)
    target_compile_options(NutBlast PUBLIC -O3) # the emscripten manual recommends this
else()
    FetchContent_Declare(datachannel
        GIT_REPOSITORY https://github.com/paullouisageneau/libdatachannel.git
        GIT_TAG v0.24.2
        EXCLUDE_FROM_ALL)
    list(APPEND LIBS datachannel-static)
endif()

FetchContent_MakeAvailable(datachannel json)
target_link_libraries(NutBlast PUBLIC ${LIBS})

if(NUTBLAST_BUILD_TEST)
    FetchContent_Declare(raylib
        GIT_REPOSITORY https://github.com/raysan5/raylib.git
        GIT_TAG 5.5
        EXCLUDE_FROM_ALL)

    FetchContent_Declare(S_tructures
        GIT_REPOSITORY https://github.com/Schwungus/S_tructures.git
        GIT_TAG dbb1697c102d17f8da67887a6fe85f2d1c94e512
        EXCLUDE_FROM_ALL)

    FetchContent_MakeAvailable(raylib S_tructures)

    add_executable(NutBlast-test ${CMAKE_CURRENT_SOURCE_DIR}/NutBlast-test.c)
    target_link_libraries(NutBlast-test PRIVATE NutBlast raylib S_tructures)
endif()
