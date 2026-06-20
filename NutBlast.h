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
#include <stddef.h>
#include <stdint.h>

#ifdef NUTBLAST_DEV_LOCALHOST
#define NUTBLAST_DEFAULT_SERVER "ws://localhost:36900/"
#else
#define NUTBLAST_DEFAULT_SERVER "wss://nutblast.schwung.us/v1"
#endif

#define NUTBLAST_MAX_PLAYERS (16)

// NOTE: make sure to sync this with `src/main.rs`.
typedef uint64_t NutBlast_ID;
typedef uint8_t NutBlast_ChannelID;

typedef struct {
    const char* data;
    NutBlast_ID from;
    size_t size;
} NutBlast_Message;

typedef struct {
    NutBlast_ID id;
    uint8_t players, capacity;
} NutBlast_Lobby;

/// Cleans up the resources that were allocated by NutBlast. Call this at the end of your program.
void NutBlast_Cleanup();

/// Returns true if you are connected to a NutBlaster and ready to accept connections from peers.
bool NutBlast_IsReady();

/// Sets the NutBlaster address.
void NutBlast_SetNutBlaster(const char*);

/// Sets the maximum amount of channels to receive messages on. Defaults to 1 channel if unspecified.
void NutBlast_SetMaxChannels(NutBlast_ChannelID);

/// Registers a callback to fire when you are successfully connected to the NutBlaster.
void NutBlast_OnConnected(void (*)());

/// Registers a callback to fire when you are disconnected from the NutBlaster.
///
/// Receives a disconnection reason, or null upon a graceful disconnection.
void NutBlast_OnDisconnected(void (*)(const char*));

/// Registers a callback to fire whenever a new peer connects to your machine.
void NutBlast_OnPlayerJoined(void (*)(NutBlast_ID));

/// Registers a callback to fire whenever a peer disconnects from your machine.
void NutBlast_OnPlayerLeft(void (*)(NutBlast_ID));

/// Registers a callback to fire whenever `NutBlast_FindLobbies` receives a list of lobbies.
void NutBlast_OnLobbiesFound(void (*)(const NutBlast_Lobby*, size_t));

/// Returns true and copies the incoming message if there is a message waiting in the queue for the specified channel.
bool NutBlast_NextMessage(NutBlast_ChannelID, NutBlast_Message*);

/// Returns true if the peer with the specified ID has a connection established to our machine.
bool NutBlast_PeerAlive(NutBlast_ID);

/// Sends a null-terminated string to the specified peer. Failures are silent. Delivery is not guaranteed.
///
/// Set `size` to -1 to assume `msg` is a zero-terminated string.
void NutBlast_SendTo(NutBlast_ChannelID chan, NutBlast_ID peer, const char* msg, int size);

/// A reliable-delivery version of `NutBlast_SendTo`, which see.
void NutBlast_SendReliablyTo(NutBlast_ChannelID chan, NutBlast_ID peer, const char* msg, int size);

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
void NutBlast_Join(NutBlast_ID id);

/// Hosts a lobby with a given ID and maximum player count.
///
/// Call `NutBlast_SetMaxPlayers()` if you need to set a different player-count later.
///
/// Pass 0 for the ID if you want NutBlast to generate one for you.
void NutBlast_Host(NutBlast_ID id, int players);

/// Requests a lobby list from the NutBlaster. Fires `NutBlast_OnLobbiesFound` after receiving a result.
void NutBlast_FindLobbies();

/// Disconnects you from the lobby if you are in one, and resets the networking state.
void NutBlast_Disconnect();

/// Returns the amount of players that are in the lobby, including yourself.
int NutBlast_GetPlayerCount();

/// Returns your player's ID.
NutBlast_ID NutBlast_GetOurID();

/// Returns the lobby's ID.
NutBlast_ID NutBlast_GetLobbyID();

/// Returns the lobby's master's ID.
NutBlast_ID NutBlast_GetMasterID();

/// Returns a 0-terminated array of IDs of the players that are in the lobby, except our own.
const NutBlast_ID* NutBlast_GetPlayerIDs();

/// Returns true if the player identified by their ID is in the lobby, and false otherwise.
bool NutBlast_IsPlayerAlive(NutBlast_ID);

/// Returns peer's metadata as a null-terminated string.
const char* NutBlast_GetPeerField(NutBlast_ID peer, const char* name);

/// Sets our peer's metadata to a null-terminated string.
void NutBlast_SetPeerField(const char* name, const char* value);

/// Returns lobby metadata as a null-terminated string.
const char* NutBlast_GetLobbyField(const char* name);

/// Sets lobby metadata to a null-terminated string.
void NutBlast_SetLobbyField(const char* name, const char* value);

uint64_t NutBlast_TimeNS();

void NutBlast_SleepMS(int);

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // NUTBLAST_H
