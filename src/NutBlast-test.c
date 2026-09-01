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

#include <stdlib.h>

#include <raylib.h>

#define S_TRUCTURES_IMPLEMENTATION
#include <S_tructures.h>

#include <NutBlast.h>

#define TICKRATE (60)

#ifdef __EMSCRIPTEN__
#define EMS (1)
#else
#define EMS (0)
#endif

static const char* names[] = {"Ninja", "Marsoyob", "Trollga", "Ficus", "Caccus", "jrb012345", "Utley"};

static const char* motds[] = {
    "When life gives you lemons, make lemonade.",
    "Live every day like it's your last.",
    "Be yourself. Everyone else is already taken.",
    "Be the change you wish to see in the world.",
    "Don't sweat the small stuff.",
};

enum {
    CHAN_POS,
    CHAN_CHAT,
    CHAN_MAX,
};

static const int psize = 20;

typedef struct {
    int x, y;
    Color color;
} Player;

static TinyMap players = {0};

static void reset() {
    FreeTinyMap(&players);
}

static void restart() {
    reset();

    Player us = {
        .x = (GetScreenWidth() - psize) / 2,
        .y = (GetScreenHeight() - psize) / 2,
        .color = RED,
    };

    TinyMapPut(&players, NutBlast_GetPlayerID(), &us, sizeof(us));
}

static void on_player_joined(NutBlast_ID id) {
    TraceLog(LOG_INFO, "Hi, %s!", NutBlast_GetPlayerField(id, NUTBLAST_FIELD_PLAYER_NAME));

    Player p = {.x = -psize, .y = -psize, .color = GREEN};
    TinyMapPut(&players, id, &p, sizeof(p));
}

static void on_player_left(NutBlast_ID id, NutBlast_Reason reason) {
    TraceLog(LOG_INFO, "Bye, %s! (%s)", NutBlast_GetPlayerField(id, NUTBLAST_FIELD_PLAYER_NAME), reason.code);
    TinyMapErase(&players, id);
}

static void draw_players() {
    TINY_MAP_FOREACH (&players, bucket) {
        const Player* p = bucket.data;
        DrawRectangle(p->x - psize / 2, p->y - psize / 2, psize, psize, p->color);
    }
}

static void draw_gui() {
    const int fs = 28;
    int i = 0;

    for (const NutBlast_ID* ptr = NutBlast_ListPlayers(); *ptr; ptr++) {
        const NutBlast_ID id = *ptr;
        const char* name = NutBlast_GetPlayerField(id, NUTBLAST_FIELD_PLAYER_NAME);

        if (name) {
            const char* fmt = TextFormat("%s (%dms)", name, NutBlast_PlayerPing(id));
            DrawText(fmt, GetScreenWidth() - MeasureText(fmt, fs) - 5, fs * i, fs, BLACK);
        }

        i++;
    }

    DrawText("H to host, J to join, U to swarm, K to reset", 0, GetScreenHeight() - fs * 2, fs, BLACK);
    DrawText("T to chat (reliable), L to kick everyone", 0, GetScreenHeight() - fs * 1, fs, BLACK);
}

static void move_our_rect() {
    Player* p = (Player*)TinyMapGet(&players, NutBlast_GetPlayerID());

    if (!p)
        return;

    const int vel = 300 / TICKRATE;
    p->x += (IsKeyDown(KEY_RIGHT) - IsKeyDown(KEY_LEFT)) * vel;
    p->y += (IsKeyDown(KEY_DOWN) - IsKeyDown(KEY_UP)) * vel;
}

static void send_our_position() {
    const Player* p = (const Player*)TinyMapGet(&players, NutBlast_GetPlayerID());

    if (!p)
        return;

    for (const NutBlast_ID* ptr = NutBlast_ListPlayers(); *ptr; ptr++) {
        const NutBlast_ID id = *ptr;

        if (id != NutBlast_GetPlayerID()) {
            static char buf[32] = "";
            snprintf(buf, sizeof(buf), "%d:%d", p->x, p->y);
            NutBlast_SendTo(CHAN_POS, id, buf, -1);
        }
    }
}

static void maybe_chat() {
    if (!IsKeyPressed(KEY_T))
        return;

    for (const NutBlast_ID* ptr = NutBlast_ListPlayers(); *ptr; ptr++) {
        const NutBlast_ID id = *ptr;

        if (id != NutBlast_GetPlayerID())
            NutBlast_SendReliablyTo(CHAN_CHAT, id, "Hello!", -1);
    }
}

static void maybe_kick() {
    if (!IsKeyPressed(KEY_L))
        return;

    for (const NutBlast_ID* ptr = NutBlast_ListPlayers(); *ptr; ptr++) {
        const NutBlast_ID id = *ptr;

        if (id != NutBlast_GetPlayerID())
            NutBlast_Kick(id);
    }
}

static void maybe_spam() {
    if (IsKeyPressed(KEY_P))
        for (int i = 0; i < 100; i++)
            NutBlast_SetPlayerField("ninja", "tone5");
}

static void recv_stuff() {
    NutBlast_Message msg = {0};

    while (NutBlast_NextMessage(CHAN_POS, &msg)) {
        Player* p = (Player*)TinyMapGet(&players, msg.from);

        if (p)
            sscanf(msg.data, "%d:%d", &p->x, &p->y);
    }

    while (NutBlast_NextMessage(CHAN_CHAT, &msg))
        TraceLog(LOG_INFO, "chat <%s> %s", NutBlast_GetPlayerField(msg.from, NUTBLAST_FIELD_PLAYER_NAME), msg.data);
}

static void on_disconnected(NutBlast_Reason reason) {
    TraceLog(reason.err ? LOG_ERROR : LOG_INFO, "%s (%s)", reason.msg, reason.code);
    reset();
}

static int nb_to_rl(NutBlast_LogLevel level) {
    switch (level) {
    case NB_LogTrace:
        return LOG_TRACE;
    case NB_LogInfo:
        return LOG_INFO;
    case NB_LogError:
        return LOG_ERROR;
    }

    return 0;
}

static void rl_logger(NutBlast_LogLevel level, const char* line) {
    TraceLog(nb_to_rl(level), "%s", line);
}

int main(int argc, char* argv[]) {
    NutBlast_SetLogger(rl_logger);

    NutBlast_Init((NutBlast_InitOptions){
        .game_id = "NutBlast Test",
        .max_channels = CHAN_MAX,
    });

    if (argc > 1)
        NutBlast_SetNutBlasterAddress(argv[1]);

    InitWindow(800, 600, "NutBlast Test");

    SetTargetFPS(TICKRATE);
    SetExitKey(EMS ? KEY_NULL : KEY_ESCAPE);
    SetRandomSeed(NutBlast_TimeNS());

    NutBlast_SetLobbyField(NUTBLAST_FIELD_LOBBY_NAME, "NutBlast Test");
    NutBlast_SetLobbyField("motd", motds[GetRandomValue(0, sizeof(motds) / sizeof(*motds) - 1)]);
    NutBlast_SetPlayerField(NUTBLAST_FIELD_PLAYER_NAME, names[GetRandomValue(0, sizeof(names) / sizeof(*names) - 1)]);
    reset();

    NutBlast_OnReady(restart);
    NutBlast_OnDisconnected(on_disconnected);
    NutBlast_OnPlayerJoined(on_player_joined);
    NutBlast_OnPlayerLeft(on_player_left);

    static NutBlast_ID lid = 0;
    ((char*)(&lid))[0] = 't';
    ((char*)(&lid))[1] = 'e';
    ((char*)(&lid))[2] = 's';
    ((char*)(&lid))[3] = 't';

    while (!WindowShouldClose()) {
        if (IsKeyPressed(KEY_H))
            NutBlast_Host((NutBlast_HostOptions){.lobby_id = lid, .max_players = 4});
        else if (IsKeyPressed(KEY_J))
            NutBlast_Join(lid);
        else if (IsKeyPressed(KEY_U))
            NutBlast_JoinSwarm();
        else if (IsKeyPressed(KEY_K))
            NutBlast_Disconnect();

        move_our_rect();
        send_our_position();
        maybe_chat();
        maybe_kick();
        maybe_spam();
        NutBlast_Update();
        recv_stuff();

        BeginDrawing();
        {
            ClearBackground(RAYWHITE);
            draw_players();
            draw_gui();
        }
        EndDrawing();
    }

    CloseWindow();
    NutBlast_Cleanup();

    return EXIT_SUCCESS;
}
