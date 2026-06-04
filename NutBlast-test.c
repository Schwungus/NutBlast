#include <stdlib.h>

#include <raylib.h>

#define TICKRATE (60)

#ifdef __EMSCRIPTEN__
#define EMS (1)
#else
#define EMS (0)
#endif

int main(int argc, char* argv[]) {
    (void)argc, (void)argv;

    InitWindow(800, 600, "NutBlast Test");

    SetTargetFPS(TICKRATE);
    SetExitKey(EMS ? KEY_NULL : KEY_ESCAPE);

    while (!WindowShouldClose()) {
        BeginDrawing();
        ClearBackground(RAYWHITE);
        EndDrawing();
    }

    CloseWindow();

    return EXIT_SUCCESS;
}
