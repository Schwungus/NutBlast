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

#ifndef NUTBLAST_H
#define NUTBLAST_H

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#include <stdbool.h>
#include <stdint.h>

#define NUTBLAST_DEFAULT_SERVER "wss://nutblast.schwung.us/v1"

#define NUTBLAST_MAX_PLAYERS (16)

// NOTE: make sure to sync this with `src/main.rs`.
typedef char NutBlast_PlayerID[4], NutBlast_GameID[16], NutBlast_LobbyID[32];

/// Sets the NutBlaster address.
void NutBlast_SetNutBlaster(const char*);

/// Call this every frame to send, receive, and process data from the NutBlaster and the peers.
void NutBlast_Update();

/// Sets a game-id which is used to differentiate the lobbies between different games.
void NutBlast_SetGameID(const char*);

/// Sets a maximum player-count accepted by the lobby. No effect if you aren't the lobby's master.
void NutBlast_SetMaxPlayers(int);

/// Joins a lobby by its ID. Note that different games have different sets of lobbies.
void NutBlast_Join(const char* id);

/// Hosts a lobby with a given ID and maximum player count.
///
/// Call `NutBlast_SetMaxPlayers()` if you need to set a different player-count later.
void NutBlast_Host(const char* id, int players);

/// Disconnects you from the lobby if you are in one, and resets the networking state.
void NutBlast_Disconnect();

/// Returns the amount of players that are in the lobby, including yourself.
int NutBlast_GetPlayerCount();

/// Returns your player's ID.
const char* NutBlast_GetOurID();

/// Returns a NULL-terminated array of IDs of the players that are in the lobby.
const char** NutBlast_GetPlayerIDs();

/// Returns true if the player identified by their ID is in the lobby, and false otherwise.
bool NutBlast_IsPlayerAlive(const char*);

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // NUTBLAST_H
