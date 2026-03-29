#ifndef TRANSFER_ENGINE_P2P_TRANSFER_EXPORT_H
#define TRANSFER_ENGINE_P2P_TRANSFER_EXPORT_H

#if defined(P2P_TRANSFER_STATIC_DEFINE)
#define P2P_TRANSFER_EXPORT
#else
#if defined(_WIN32) || defined(__CYGWIN__)
#if defined(OPEN_BUILD_PROJECT)
#define P2P_TRANSFER_EXPORT __declspec(dllexport)
#else
#define P2P_TRANSFER_EXPORT __declspec(dllimport)
#endif
#else
#if __GNUC__ >= 4
#define P2P_TRANSFER_EXPORT __attribute__((visibility("default")))
#else
#define P2P_TRANSFER_EXPORT
#endif
#endif
#endif

#endif  // TRANSFER_ENGINE_P2P_TRANSFER_EXPORT_H
