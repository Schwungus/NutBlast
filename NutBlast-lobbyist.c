#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#include <NutBlast.h>

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

int main(int argc, char* argv[]) {
    if (argc > 1)
        NutBlast_SetNutBlaster(argv[1]);
    NutBlast_SetGameID(argc > 2 ? argv[2] : "NutBlast Test");

    static const size_t LIMOZ = 10;
    NutBlast_OnLobbiesFound(on_lobbies_found);
    NutBlast_FindLobbies(LIMOZ);

    int timer = 0;

    for (;;) {
        NutBlast_Update();
        NutBlast_SleepMS(100);

        if (timer++ >= 50) {
            NutBlast_FindLobbies(LIMOZ);
            timer = 0;
        }
    }

    NutBlast_Cleanup();

    return EXIT_SUCCESS;
}
