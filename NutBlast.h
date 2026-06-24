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

typedef enum {
    NB_LogInfo,
    NB_LogError,
} NutBlast_LogLevel;

/// Converts a NutBlast-specific loglevel to string so you don't have to do it yourself.
const char* NutBlast_LogLevelToString(NutBlast_LogLevel);

/// Sets a logging callback to use by NutBlast. Set to null to reset it to the built-in default handler.
void NutBlast_SetLogger(void (*)(NutBlast_LogLevel, const char*));

typedef struct {
    const char* data;
    NutBlast_ID from;
    size_t size;
} NutBlast_Message;

typedef struct {
    const char *name, *old_value, *new_value;
} NutBlast_FieldDiff;

typedef struct {
    const char *key, *value;
} NutBlast_LobbyField;

typedef struct {
    NutBlast_ID id;
    const char* name;
    uint8_t players, capacity;
    const NutBlast_LobbyField* metadata;
    size_t field_count;
} NutBlast_Lobby;

/// Cleans up the resources that were allocated by NutBlast. Call this at the end of your program.
void NutBlast_Cleanup();

/// Returns true if you are connected to a NutBlaster and ready to accept connections from peers.
bool NutBlast_IsReady();

/// Returns the average round-trip time (in milliseconds) to the NutBlaster.
int NutBlast_ServerPing();

/// Returns the average round-trip time (in milliseconds) to the specified peer.
int NutBlast_PlayerPing(NutBlast_ID);

/// Sets the NutBlaster address.
///
/// Pass NULL to reset the address to its default value.
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

/// Registers a callback to fire whenever the lobby's master changed.
///
/// The new master's ID is available through `NutBlast_GetMasterID`. The old master's ID is passed to the callback, and
/// that ID may point to a dead peer.
void NutBlast_OnMasterChanged(void (*)(NutBlast_ID));

/// Registers a callback to fire whenever a peer's metadata field changes.
///
/// The old or new value may be null.
void NutBlast_OnPeerMetadataChanged(void (*)(NutBlast_ID, NutBlast_FieldDiff));

/// Registers a callback to fire whenever the lobby's metadata changes.
///
/// The old or new value may be null.
void NutBlast_OnLobbyMetadataChanged(void (*)(NutBlast_FieldDiff));

/// Returns true and copies the incoming message if there is a message waiting in the queue for the specified channel.
bool NutBlast_NextMessage(NutBlast_ChannelID, NutBlast_Message*);

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

/// Hosts a lobby with a given ID, name, maximum player count and visibility.
///
/// Pass 0 for `id` if you want NutBlast to generate one for you.
/// `name` must not be NULL or empty.
/// Call `NutBlast_SetMaxPlayers()` if you need to set a different player-count later.
/// Call `NutBlast_SetListed()` if you need to change the visibility later.
void NutBlast_Host(NutBlast_ID id, const char* name, int players, bool listed);

/// Requests a lobby list from the NutBlaster. Fires `NutBlast_OnLobbiesFound` after receiving a result.
void NutBlast_FindLobbies();

/// Disconnects you from the lobby if you are in one, and resets the networking state.
void NutBlast_Disconnect();

/// Kicks the specified player if you are the lobby's master. Silently ignored otherwise.
void NutBlast_Kick(NutBlast_ID);

/// Lists or unlists your lobby from public lobby listings.
void NutBlast_SetListed(bool);

/// Returns true if you are in a publicly listed lobby, and false otherwise.
bool NutBlast_IsListed();

/// Returns the amount of players that are in the lobby, including yourself.
int NutBlast_GetPlayerCount();

/// Returns the maximum player count the current lobby can take, or 0 if you aren't in a lobby.
int NutBlast_GetMaxPlayers();

/// Returns your player's ID.
NutBlast_ID NutBlast_GetOurID();

/// Returns the lobby's ID.
NutBlast_ID NutBlast_GetLobbyID();

/// Returns the lobby's master's ID.
NutBlast_ID NutBlast_GetMasterID();

/// Returns a 0-terminated array of IDs of every player in the lobby, including yourself.
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
