// This is free and unencumbered software released into the public domain.
//
// Anyone is free to copy, modify, publish, use, compile, sell, or
// distribute this software, either in source code form or as a compiled
// binary, for any purpose, commercial or non-commercial, and by any
// means.
//
// In jurisdictions that recognize copyright laws, the author or authors
// of this software dedicate any and all copyright interest in the
// software to the public domain. We make this dedication for the benefit
// of the public at large and to the detriment of our heirs and
// successors. We intend this dedication to be an overt act of
// relinquishment in perpetuity of all present and future rights to this
// software under copyright law.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
//
// For more information, please refer to <https://unlicense.org>

#include <optional>
#include <string>
#include <unordered_map>

#include <nlohmann/json.hpp>

#include <NutBlast.h>

struct Peer {};

static std::string gid = "", pid = "";
static std::optional<std::string> blaster, lid;
static bool host = false;

static Peer self;
static std::unordered_map<std::string, Peer> peers;

template <typename... T> static inline void info(const char* fmt, T... args) {
    std::printf(fmt, args...);
}

static std::string_view get_pid() {
    if (pid.empty())
        for (size_t i = 0; i < sizeof(NutBlast_PlayerID); i++)
            pid.push_back(static_cast<char>('A' + std::rand() % ('Z' - 'A' + 1)));

    return pid;
}

static std::string_view get_blaster() {
    if (blaster == std::nullopt) {
        info("Using the default NutBlaster server as none was explicitly specified: %s", NUTBLAST_DEFAULT_SERVER);
        blaster = NUTBLAST_DEFAULT_SERVER;
    }

    return *blaster;
}

static int max_players = NUTBLAST_MAX_PLAYERS;

extern "C" void NutBlast_SetNutBlaster(const char* blaster) {
    ::blaster = blaster;
}

extern "C" void NutBlast_SetGameID(const char* gid) {
    ::gid = gid;
}

extern "C" void NutBlast_SetMaxPlayers(int max) {
    if (max > 1 && max <= NUTBLAST_MAX_PLAYERS)
        ::max_players = max;
}

static void join_pro(const char* id, bool host) {
    get_pid(), get_blaster();
    ::lid = id, ::host = host;
}

extern "C" void NutBlast_Join(const char* id) {
    join_pro(id, false);
}

extern "C" void NutBlast_Host(const char* id, int max) {
    NutBlast_SetMaxPlayers(max);
    join_pro(id, true);
}

extern "C" int NutBlast_GetPlayerCount() {}

extern "C" const char* NutBlast_GetPlayerID(int idx) {}

extern "C" bool NutBlast_IsPlayerAlive(const char*) {}

extern "C" void NutBlast_Update() {}
