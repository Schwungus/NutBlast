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

#define NUTBLAST_DEFAULT_SERVER (NUTBLAST_DEV_LOCALHOST ? "ws://localhost:36900/" : "wss://nutblast.schwung.us/v1")

// NOTE: make sure to sync these limits with `src/main.rs`.

#define NUTBLAST_MAX_PLAYERS (16)
#define NUTBLAST_MAX_FIELDS (16)
#define NUTBLAST_FIELD_NAME_MAX (255)
#define NUTBLAST_FIELD_VALUE_MAX (8191)

// NOTE: same with these field and error macros for any special behavior:

/// A universally agreed upon "lobby name" field.
#define NUTBLAST_FIELD_LOBBY_NAME "NutBlast.lobby.name"

/// A universally agreed upon "player name" field.
#define NUTBLAST_FIELD_PLAYER_NAME "NutBlast.player.name"

/// No error (i.e. graceful disconnect).
#define NUTBLAST_ERROR_OK "ok"

/// Sent a binary message to NutBlaster.
#define NUTBLAST_ERROR_BINARY_UNSUPPORTED "binary_unsupported"

/// Sent an invalid JSON to NutBlaster.
#define NUTBLAST_ERROR_BAD_JSON "bad_json"

/// Sent an unknown, invalid or inappropriate payload to NutBlaster.
#define NUTBLAST_ERROR_BAD_PAYLOAD "bad_payload"

/// Sent too many payloads to NutBlaster.
#define NUTBLAST_ERROR_RATE_LIMITED "rate_limited"

/// Attempted to host a lobby with an occupied ID.
#define NUTBLAST_ERROR_LOBBY_EXISTS "lobby_exists"

/// Attempted to join a lobby with a non-existant ID.
#define NUTBLAST_ERROR_LOBBY_NOT_FOUND "lobby_not_found"

/// Attempted to join a full lobby.
#define NUTBLAST_ERROR_LOBBY_FULL "lobby_full"

/// Kicked by the lobby's master.
#define NUTBLAST_ERROR_KICK "kick"

/// Stayed in an empty lobby too long.
#define NUTBLAST_ERROR_INACTIVE_LOBBY "inactive_lobby"

/// A unique identifier for players & lobbies.
typedef uint64_t NutBlast_ID;

/// A channel ID for discerning P2P messages. You would usually define them with an enum.
typedef uint8_t NutBlast_ChannelID;

/// A log-level passed to custom loggers (see `NutBlast_SetLogger`).
typedef enum {
    NB_LogInfo,
    NB_LogError,
} NutBlast_LogLevel;

/// Converts a NutBlast-specific loglevel to string so you don't have to do it yourself.
const char* NutBlast_LogLevelToString(NutBlast_LogLevel);

/// Sets a logging callback to use by NutBlast. Set to null to reset it to the built-in default handler.
void NutBlast_SetLogger(void (*)(NutBlast_LogLevel, const char*));

/// A message from another peer.
typedef struct {
    const char* data;
    NutBlast_ID from;
    size_t size;
} NutBlast_Message;

/// A diff of a metadata field.
typedef struct {
    const char *name, *old_value, *new_value;
} NutBlast_FieldDiff;

/// A metadata field.
typedef struct {
    const char *key, *value;
} NutBlast_LobbyField;

/// Lobby info from the lobby finder (see `NutBlast_FindLobbies`).
typedef struct {
    NutBlast_ID id;
    uint8_t players, capacity;
    const NutBlast_LobbyField* metadata;
    size_t field_count;
} NutBlast_Lobby;

/// A kick/leave reason for a player.
typedef struct {
    bool err;
    const char *code, *msg;
} NutBlast_Reason;

/// Cleans up the resources that were allocated by NutBlast. Call this at the end of your program.
void NutBlast_Cleanup();

/// Returns true if you are connected to a NutBlaster.
bool NutBlast_IsOnline();

/// Returns true if you are connected to a NutBlaster AND ready to communicate with all current players in the lobby.
/// Note that when a new player is entering the lobby, this will go back to returning false until a connection is
/// established with them.
bool NutBlast_IsReady();

/// Returns the average round-trip time (in milliseconds) to the NutBlaster.
int NutBlast_ServerPing();

/// Returns the average round-trip time (in milliseconds) to the specified player.
int NutBlast_PlayerPing(NutBlast_ID);

/// Sets the NutBlaster address.
///
/// Pass NULL to reset the address to its default value.
void NutBlast_SetNutBlaster(const char*);

/// Sets the maximum amount of channels to receive messages on. Defaults to 1 channel if unspecified.
void NutBlast_SetMaxChannels(NutBlast_ChannelID);

/// Registers a callback to fire as soon as `NutBlast_Ready()` signals you are ready for the first time.
void NutBlast_OnReady(void (*)());

/// Registers a callback to fire when you are disconnected from the NutBlaster.
void NutBlast_OnDisconnected(void (*)(NutBlast_Reason));

/// Registers a callback to fire whenever a new player connects to your machine.
void NutBlast_OnPlayerJoined(void (*)(NutBlast_ID));

/// Registers a callback to fire whenever a player disconnects from your machine.
void NutBlast_OnPlayerLeft(void (*)(NutBlast_ID, NutBlast_Reason));

/// Registers a callback to fire whenever `NutBlast_FindLobbies` receives a list of lobbies.
void NutBlast_OnLobbiesFound(void (*)(const NutBlast_Lobby*, size_t));

/// Registers a callback to fire whenever the lobby's master changed.
///
/// The new master's ID is available through `NutBlast_GetMasterID`. The old master's ID is passed to the callback, and
/// that ID may point to a dead player.
void NutBlast_OnMasterChanged(void (*)(NutBlast_ID));

/// Registers a callback to fire whenever a player's metadata field changes.
///
/// The old or new value may be null.
void NutBlast_OnPlayerMetadataChanged(void (*)(NutBlast_ID, NutBlast_FieldDiff));

/// Registers a callback to fire whenever the lobby's metadata changes.
///
/// The old or new value may be null.
void NutBlast_OnLobbyMetadataChanged(void (*)(NutBlast_FieldDiff));

/// Returns true and copies the incoming message if there is a message waiting in the queue for the specified channel.
bool NutBlast_NextMessage(NutBlast_ChannelID, NutBlast_Message*);

/// Sends a null-terminated string to the specified player. Failures are silent. Delivery is not guaranteed.
///
/// Set `size` to -1 to assume `msg` is a zero-terminated string.
void NutBlast_SendTo(NutBlast_ChannelID chan, NutBlast_ID player, const char* msg, int size);

/// A reliable-delivery version of `NutBlast_SendTo`, which see.
void NutBlast_SendReliablyTo(NutBlast_ChannelID chan, NutBlast_ID player, const char* msg, int size);

/// Call this every frame to send, receive, and process data from the NutBlaster and the players.
void NutBlast_Update();

/// Call this to flush the output queue.
///
/// This is useful in the case you process the result of `NutBlast_Update()` (i.e. the data you received from other
/// players) and send out a response immediately after. Without `NutBlast_Flush()`, you would have to wait a whole extra
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
void NutBlast_Host(NutBlast_ID id, int players, bool listed);

/// Requests a lobby list from the NutBlaster. Fires `NutBlast_OnLobbiesFound` after receiving a result.
void NutBlast_FindLobbies(size_t);

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

/// Returns a 0-terminated array of IDs of every reachable player in the lobby, including yourself.
const NutBlast_ID* NutBlast_GetPlayerIDs();

/// Returns true if the specified player is in the lobby AND can be reached over the network, and false otherwise.
bool NutBlast_IsPlayerAlive(NutBlast_ID);

/// Returns player's metadata as a null-terminated string.
const char* NutBlast_GetPlayerField(NutBlast_ID player, const char* name);

/// Sets our player's metadata to a null-terminated string. Pass a NULL value to unset the field.
void NutBlast_SetPlayerField(const char* name, const char* value);

/// Returns lobby metadata as a null-terminated string.
const char* NutBlast_GetLobbyField(const char* name);

/// Sets lobby metadata to a null-terminated string. Pass a NULL value to unset the field.
void NutBlast_SetLobbyField(const char* name, const char* value);

/// Purges lobby & peer metadata if it concerns you that metadata is kept between sessions.
void NutBlast_PurgeMetadata();

/// Sets the specified player as the new master if you are the lobby's master. Silently ignored otherwise.
void NutBlast_SetMaster(NutBlast_ID);

/// Internal timing utility.
uint64_t NutBlast_TimeNS();

/// Internal cross-platform `Sleep` implementation.
void NutBlast_SleepMS(int);

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // NUTBLAST_H
