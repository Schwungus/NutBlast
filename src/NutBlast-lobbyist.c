#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#include <NutBlast.h>

#ifdef __EMSCRIPTEN__
#include <emscripten.h>
#endif

static void on_lobbies_found(const NutBlast_Lobby* list, size_t count) {
    printf("\n");

    if (!count)
        printf("No lober\n");

    for (size_t i = 0; i < count; i++) {
        const NutBlast_Lobby lober = list[i];
        printf("%" PRIu64 ": %u/%u\n", lober.id, lober.players, lober.capacity);
        printf("  %zu field(s):\n", lober.field_count);
        for (size_t j = 0; j < lober.field_count; j++)
            printf("    %s = %s\n", lober.metadata[j].key, lober.metadata[j].value);
    }

    printf("\n");
    fflush(stdout);
}

static const int fps = 10;
static const size_t lobbies_count = 10;
static int timer = 0;

static void mainloop() {
    NutBlast_Update();

    if (timer++ >= 5 * fps) {
        NutBlast_FindLobbies(lobbies_count);
        timer = 0;
    }
}

int main(int argc, char* argv[]) {
    NutBlast_Init((NutBlast_InitOptions){
        .game_id = argc > 2 ? argv[2] : "NutBlast Test",
    });

    if (argc > 1)
        NutBlast_SetNutBlasterAddress(argv[1]);

    NutBlast_OnLobbiesFound(on_lobbies_found);
    NutBlast_FindLobbies(lobbies_count);

#ifdef __EMSCRIPTEN__
    emscripten_set_main_loop(mainloop, 10, false);
#else
    for (;;) {
        mainloop();
        NutBlast_SleepMS(1000 / fps);
    }

    NutBlast_Cleanup();
#endif

    return EXIT_SUCCESS;
}
