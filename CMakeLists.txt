cmake_minimum_required(VERSION 3.28 FATAL_ERROR)

project(NutBlast
    VERSION 0.1.0
    LANGUAGES C CXX)

option(NUTBLAST_BUILD_TEST "Build a NutBlast test binary?" ${PROJECT_IS_TOP_LEVEL})
option(NUTBLAST_DEV_LOCALHOST "Connect to ws://localhost:36900 by default?" OFF)

set(NB_BUILD_SHARED_LIBS_ORIGINAL_VALUE ${BUILD_SHARED_LIBS})
set(BUILD_SHARED_LIBS OFF)

set(NB_CMAKE_SKIP_INSTALL_RULES_ORIGINAL_VALUE ${CMAKE_SKIP_INSTALL_RULES})
set(CMAKE_SKIP_INSTALL_RULES ON)

if(PROJECT_IS_TOP_LEVEL AND EMSCRIPTEN AND NUTBLAST_BUILD_TEST)
    set(CMAKE_EXECUTABLE_SUFFIX ".html")
    set(PLATFORM Web CACHE INTERNAL "")
endif()

include(FetchContent)

if(PROJECT_IS_TOP_LEVEL)
    set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
    set(FETCHCONTENT_QUIET OFF)
endif()

add_library(NutBlast ${CMAKE_CURRENT_SOURCE_DIR}/src/NutBlast.cpp ${CMAKE_CURRENT_SOURCE_DIR}/include/NutBlast.h)
target_include_directories(NutBlast PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(NutBlast PRIVATE c_std_11 cxx_std_23)
target_compile_definitions(NutBlast PUBLIC NUTBLAST_DEV_LOCALHOST=$<BOOL:${NUTBLAST_DEV_LOCALHOST}>)

FetchContent_Declare(json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.12.0
    OVERRIDE_FIND_PACKAGE
    EXCLUDE_FROM_ALL)

FetchContent_MakeAvailable(json)
list(APPEND NB_LIBS nlohmann_json::nlohmann_json)

set(USE_SYSTEM_JSON ON CACHE INTERNAL "")
set(USE_SYSTEM_SRTP OFF CACHE INTERNAL "")
set(USE_SYSTEM_JUICE OFF CACHE INTERNAL "")
set(USE_SYSTEM_PLOG OFF CACHE INTERNAL "")
set(NO_MEDIA ON CACHE INTERNAL "")
set(NO_EXAMPLES ON CACHE INTERNAL "")
set(NO_TESTS ON CACHE INTERNAL "")
set(USE_MBEDTLS ON CACHE INTERNAL "")
set(ENABLE_PROGRAMS OFF CACHE INTERNAL "")
set(ENABLE_TESTING OFF CACHE INTERNAL "")
set(INSTALL_DEPS_LIBS OFF CACHE INTERNAL "")
set(USE_STATIC_MBEDTLS_LIBRARY ON CACHE INTERNAL "")

set(MBEDTLS_TARGET_PREFIX "" CACHE INTERNAL "")
set(MBEDTLS_USER_CONFIG_FILE ${CMAKE_CURRENT_SOURCE_DIR}/src/mbedtls.h CACHE INTERNAL "")

if(EMSCRIPTEN)
    FetchContent_Declare(datachannel-wasm
        GIT_REPOSITORY https://github.com/paullouisageneau/datachannel-wasm.git
        GIT_TAG v0.4.0
        EXCLUDE_FROM_ALL)

    FetchContent_MakeAvailable(datachannel-wasm)
    list(APPEND NB_LIBS datachannel-wasm)
else()
    FetchContent_Declare(MbedTLS
        URL https://github.com/Mbed-TLS/mbedtls/releases/download/mbedtls-3.6.7/mbedtls-3.6.7.tar.bz2
        URL_HASH SHA256=a7e8bcbec0e6f761b4af24f25677626b35f762f68eef79c08677a363212d11f6
        EXCLUDE_FROM_ALL)

    FetchContent_MakeAvailable(MbedTLS)
    add_library(MbedTLS::MbedTLS ALIAS mbedtls)

    FetchContent_Declare(datachannel
        GIT_REPOSITORY https://github.com/paullouisageneau/libdatachannel.git
        GIT_TAG v0.24.6
        EXCLUDE_FROM_ALL)

    FetchContent_MakeAvailable(datachannel)
    list(APPEND NB_LIBS datachannel-static)
endif()

target_link_libraries(NutBlast PRIVATE ${NB_LIBS})

if(NUTBLAST_BUILD_TEST)
    # a personal raylib fork that skips `-sEXPORTED_RUNTIME_METHODS=...` entirely for emscripten
    # builds; multiple `-sEXPORTED_RUNTIME_METHODS=...` declarations don't get squashed together for
    # some bizzare reason.

    FetchContent_Declare(raylib
        GIT_REPOSITORY https://github.com/nonk123/raylib.git
        GIT_TAG 58ae9b2961cb7f14b57b70d1e40b802cb7fba0bf
        EXCLUDE_FROM_ALL)

    FetchContent_Declare(S_tructures
        GIT_REPOSITORY https://github.com/Schwungus/S_tructures.git
        GIT_TAG 484e78265ec3e58cf57bfe7aa09b53d48b60300f
        EXCLUDE_FROM_ALL)

    FetchContent_MakeAvailable(raylib S_tructures)

    add_executable(NutBlast-test ${CMAKE_CURRENT_SOURCE_DIR}/src/NutBlast-test.c)
    target_link_libraries(NutBlast-test PRIVATE NutBlast raylib S_tructures)

    add_executable(NutBlast-lobbyist ${CMAKE_CURRENT_SOURCE_DIR}/src/NutBlast-lobbyist.c)
    target_link_libraries(NutBlast-lobbyist PRIVATE NutBlast)

    if(EMSCRIPTEN)
        target_link_options(NutBlast-test PRIVATE
            -sEXPORTED_RUNTIME_METHODS=HEAPF32,HEAPU32,HEAPU8
            -sNO_DISABLE_EXCEPTION_CATCHING
            -sASYNCIFY)

        target_link_options(NutBlast-lobbyist PRIVATE
            -sEXPORTED_RUNTIME_METHODS=HEAPU32,HEAPU8
            -sEXPORTED_FUNCTIONS=_main,_malloc,_free)
    endif()
endif()

set(CMAKE_SKIP_INSTALL_RULES ${NB_CMAKE_SKIP_INSTALL_RULES_ORIGINAL_VALUE})
set(BUILD_SHARED_LIBS ${NB_BUILD_SHARED_LIBS_ORIGINAL_VALUE})
