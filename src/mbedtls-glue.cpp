// google ai mode code :skull:

#ifdef _WIN32

#include <mutex>

#include "mbedtls/threading.h"

// Define the context structure using standard C++ types
struct mbedtls_threading_mutex_t {
    std::mutex* mut;
    int is_valid;
};

static extern "C" void mbedtls_threading_init_alt(mbedtls_threading_mutex_t* mutex) {
    if (!mutex)
        return;
    mutex->mut = new std::mutex();
    mutex->is_valid = 1;
}

static extern "C" void mbedtls_threading_free_alt(mbedtls_threading_mutex_t* mutex) {
    if (!mutex || !mutex->is_valid)
        return;
    delete mutex->mut;
    mutex->is_valid = 0;
}

static extern "C" int mbedtls_threading_mutex_lock_alt(mbedtls_threading_mutex_t* mutex) {
    if (!mutex || !mutex->is_valid)
        return -1;
    mutex->mut->lock();
    return 0;
}

static extern "C" int mbedtls_threading_mutex_unlock_alt(mbedtls_threading_mutex_t* mutex) {
    if (!mutex || !mutex->is_valid)
        return -1;
    mutex->mut->unlock();
    return 0;
}

void NutBlast_InitMbedTlsAlt() {
    mbedtls_threading_set_alt(mbedtls_threading_init_alt, mbedtls_threading_free_alt, mbedtls_threading_mutex_lock_alt,
        mbedtls_threading_mutex_unlock_alt);
}

void NutBlast_CleanupMbedTlsAlt() {
    mbedtls_threading_free_alt();
}

#endif
