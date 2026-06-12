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

#ifdef NUTBLAST_DEV_LOCALHOST
#define NUTBLAST_DEFAULT_SERVER "ws://localhost:36900/"
#else
#define NUTBLAST_DEFAULT_SERVER "wss://nutblast.schwung.us/v1"
#endif

#define NUTBLAST_MAX_PLAYERS (16)

// NOTE: make sure to sync this with `src/main.rs`.
typedef char NutBlast_PlayerID[4], NutBlast_GameID[16], NutBlast_LobbyID[32];

/// Sets the NutBlaster address.
void NutBlast_SetNutBlaster(const char*);

/// Registers a callback to fire when you are successfully connected to the NutBlaster.
void NutBlast_OnConnected(void (*)());

/// Registers a callback to fire when you are disconnected from the NutBlaster.
void NutBlast_OnDisconnected(void (*)(const char*));

/// Registers a callback to fire whenever a new peer connects to your machine.
void NutBlast_OnPlayerJoined(void (*)(const char*));

/// Registers a callback to fire whenever a peer disconnects from your machine.
void NutBlast_OnPlayerLeft(void (*)(const char*));

/// Registers a callback to fire whenever you receive a message from a peer.
void NutBlast_OnMessage(void (*)(const char* from, const char* message));

/// Sends a null-terminated string to the specified peer. Failures are silent. Delivery is not guaranteed.
void NutBlast_SendTo(const char* peer, const char* msg);

/// Call this every frame to send, receive, and process data from the NutBlaster and the peers.
void NutBlast_Update();

/// Call this to flush the output queue.
///
/// This is useful in the case you process the result of `NutBlast_Update()` (i.e. the data you received from other
/// peers) and send out a response immediately after. Without `NutBlast_Flush()`, you would have to wait a whole extra
/// tick for the next `NutBlast_Update()` call to flush those packets.
void NutBlast_Flush();

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

/// Returns the lobby's master's ID.
const char* NutBlast_GetMasterID();

/// Returns a NULL-terminated array of IDs of the players that are in the lobby.
const char** NutBlast_GetPlayerIDs();

/// Returns true if the player identified by their ID is in the lobby, and false otherwise.
bool NutBlast_IsPlayerAlive(const char*);

/// Returns peer's metadata as a null-terminated string.
const char* NutBlast_GetPeerField(const char* peer, const char* name);

/// Sets our peer's metadata to a null-terminated string.
void NutBlast_SetPeerField(const char* name, const char* value);

/// Returns lobby metadata as a null-terminated string.
const char* NutBlast_GetLobbyField(const char* name);

/// Sets lobby metadata to a null-terminated string.
void NutBlast_SetLobbyField(const char* name, const char* value);

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // NUTBLAST_H
