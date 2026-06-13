#include <stdio.h>
#include <stdlib.h>

#include <NutBlast.h>

static void on_found(const NutBlast_Lobby* list, size_t count) {
    printf("\n");

    if (!count)
        printf("No lober\n");

    for (size_t i = 0; i < count; i++) {
        const NutBlast_Lobby lober = list[i];
        printf("%s: %d/%d\n", lober.name, lober.players, lober.capacity);
    }

    printf("\n");
    fflush(stdout);
}

int main(int argc, char* argv[]) {
    NutBlast_SetGameID(argc > 1 ? argv[1] : "NutBlast Test");

    NutBlast_OnLobbiesFound(on_found);
    NutBlast_FindLobbies();

    int count = 0;

    for (;;) {
        NutBlast_Update();
        NutBlast_SleepMS(100);

        if (count++ >= 50) {
            NutBlast_FindLobbies();
            count = 0;
        }
    }

    return EXIT_SUCCESS;
}
