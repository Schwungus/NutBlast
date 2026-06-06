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

#include <NutBlast.h>

#define TICKRATE (60)

#ifdef __EMSCRIPTEN__
#define EMS (1)
#else
#define EMS (0)
#endif

// internal utility func.
extern uint64_t NutBlast_TimeNS();

static const char* names[] = {"Ninja", "Marsoyob", "Trollga", "Ficus", "Caccus", "jrb012345", "Utley"};

int main(int argc, char* argv[]) {
    (void)argc, (void)argv;

    NutBlast_SetGameID("NutBlast Test");

    InitWindow(800, 600, "NutBlast Test");

    SetTargetFPS(TICKRATE);
    SetExitKey(EMS ? KEY_NULL : KEY_ESCAPE);
    SetRandomSeed(NutBlast_TimeNS());

    NutBlast_SetPeerField("NAME", names[GetRandomValue(0, sizeof(names) / sizeof(*names) - 1)]);

    while (!WindowShouldClose()) {
        if (IsKeyPressed(KEY_H))
            NutBlast_Host("test", 2);
        else if (IsKeyPressed(KEY_J))
            NutBlast_Join("test");
        else if (IsKeyPressed(KEY_K))
            NutBlast_Disconnect();

        NutBlast_Update();

        BeginDrawing();
        {
            ClearBackground(RAYWHITE);

            const int fs = 28;
            int i = 0;

            const char* name = NutBlast_GetPeerField(NutBlast_GetOurID(), "NAME");
            if (name)
                DrawText(name, GetScreenWidth() - MeasureText(name, fs), fs * i, fs, BLACK);
            i++;

            for (const char** ptr = NutBlast_GetPlayerIDs(); *ptr; ptr++) {
                name = NutBlast_GetPeerField(*ptr, "NAME");
                if (name)
                    DrawText(name, GetScreenWidth() - MeasureText(name, fs), fs * i, fs, BLACK);
                i++;
            }

            DrawText("H to host, J to join, K to reset", 0, GetScreenHeight() - fs, fs, BLACK);
        }
        EndDrawing();
    }

    CloseWindow();

    return EXIT_SUCCESS;
}
