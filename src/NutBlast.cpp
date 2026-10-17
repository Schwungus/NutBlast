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

#include <chrono>
#include <cstring>
#include <deque>
#include <format>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <rtc/rtc.hpp>
#include <rtc/websocket.hpp>

#include <nlohmann/json.hpp>

#include <NutBlast.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/val.h>
#endif

#ifdef _WIN32
#include <windows.h>
#else
#include <errno.h>
#endif

extern "C" {
    bool NutBlast_IgnoreICE = false;
}

namespace NutBlast {
    template <typename... Args> class Callback {
        void (*fn)(Args...) = nullptr;

      public:
        Callback() = default;

        Callback& operator=(void (*fn)(Args...)) {
            this->fn = fn;
            return *this;
        }

        void operator()(const std::decay_t<Args>&... args) {
            if (fn)
                fn(args...);
        }
    };
} // namespace NutBlast

#define MakeCb(name, ident, ...)                                                                                       \
    namespace NutBlast {                                                                                               \
        Callback<__VA_ARGS__> ident;                                                                                   \
    }                                                                                                                  \
                                                                                                                       \
    extern "C" void NutBlast_##name(void (*cb)(__VA_ARGS__)) {                                                         \
        NutBlast::ident = cb;                                                                                          \
    }

MakeCb(OnReady, on_ready);
MakeCb(OnDisconnected, on_disconnected, NutBlast_Reason);
MakeCb(OnPlayerJoined, on_player_joined, NutBlast_PlayerID);
MakeCb(OnPlayerLeft, on_player_left, NutBlast_PlayerID, NutBlast_Reason);
MakeCb(OnLobbiesFound, on_lobbies_found, const NutBlast_Lobby*, size_t);
MakeCb(OnMasterChanged, on_master_changed, NutBlast_PlayerID);
MakeCb(OnPlayerMetadataChanged, on_player_meta_changed, NutBlast_PlayerID, NutBlast_FieldDiff);
MakeCb(OnLobbyMetadataChanged, on_lobby_meta_changed, NutBlast_FieldDiff);

namespace NutBlast {
    constexpr const std::size_t MAX_CHANNELS = 16;
    constexpr const int MAX_RENEGOTIATION_ATTEMPTS = 3;

    using Metadata = std::unordered_map<std::string, std::string>;

    namespace ns {
        constexpr const std::uint64_t second = 1000000000, milli = second / 1000;
    };

    namespace interval {
        constexpr const std::uint64_t beat = ns::second / 62, ping = ns::second, sdp_timeout = 5 * ns::second;
    };

    template <typename T> class Locked {
        std::mutex mutex;
        T value;

      public:
        class LockGuard {
            std::lock_guard<decltype(mutex)> lock;
            T& ref;

          public:
            LockGuard(T& ref, decltype(mutex)& mutex) : ref(ref), lock(mutex) {}

            T& operator*() {
                return ref;
            }

            T* operator->() {
                return &ref;
            }
        };

        Locked() = default;
        Locked(const T& value) : value(value) {}
        ~Locked() = default;

        LockGuard lock() {
            return LockGuard(value, mutex);
        }
    };

    template <typename T> T copy_and_clear(T& obj) {
        auto copy = obj;
        obj.clear();
        return copy;
    }

    class Pinger {
        std::uint64_t last_ping = 0, last_roundtrip = 0;

      public:
        int millis() const {
            return (int)last_roundtrip;
        }

        void reset() {
            last_ping = last_roundtrip = 0;
        }

        void ping() {
            last_ping = NutBlast_TimeNS();
        }

        void pong() {
            if (last_ping)
                last_roundtrip = (NutBlast_TimeNS() - last_ping) / (2 * ns::milli);
        }
    };

    class Once {
        bool f_fired = false;

      public:
        bool fired() const {
            return f_fired;
        }

        void reset() {
            f_fired = false;
        }

        explicit operator bool() {
            if (f_fired) {
                return false;
            } else {
                f_fired = true;
                return true;
            }
        }
    };

    struct ByeReason {
        static const ByeReason OK;

        bool err = false;
        std::string code, msg;

        ByeReason(const std::string& code, const std::string& msg) : code(code), msg(msg) {}

        ByeReason(const nlohmann::json& obj)
            : err(obj.at("type") == "violation"), code(obj.at("code")), msg(obj.at("msg")) {}

        operator NutBlast_Reason() const {
            return {.err = err, .code = code.c_str(), .msg = msg.c_str()};
        }
    };

    const ByeReason ByeReason::OK(NUTBLAST_ERROR_OK, "Graceful disconnection");

    struct Player : std::enable_shared_from_this<Player> {
        Once fire_join, init;

        const NutBlast_PlayerID pid;
        const std::uint64_t birth;

        Pinger pinger;
        Metadata meta;

        std::shared_ptr<rtc::PeerConnection> pc = nullptr;
        std::shared_ptr<rtc::DataChannel> reliable_dc = nullptr, unreliable_dc = nullptr, ping_dc = nullptr;
        Locked<std::vector<rtc::Candidate>> outgoing_candidates;

        std::optional<std::uint64_t> sdp_timeout = std::nullopt;
        int renegotiation_attempts = 0;

        Player(NutBlast_PlayerID pid, const Metadata& meta, std::uint64_t birth) : pid(pid), meta(meta), birth(birth) {}
        ~Player();

        void engage();

        bool is_offerer() const {
            return NutBlast_GetPlayerID() > pid;
        }

        bool is_ready() const {
            return unreliable_dc && reliable_dc && ping_dc;
        }

        void drain_incoming_offers_and_candidates();
    };

    struct Message {
        NutBlast_PlayerID from;
        rtc::binary bytes;

        Message() = default;
        Message(NutBlast_PlayerID from, const rtc::binary& bytes) : from(from), bytes(bytes) {}
    };

    class Ticker {
        const std::uint64_t interval;
        std::uint64_t last_tick = 0;

      public:
        Ticker(std::uint64_t interval) : interval(interval) {}

        explicit operator bool() {
            const std::uint64_t now = NutBlast_TimeNS();

            if (!last_tick || now - last_tick >= interval) {
                last_tick = now;
                return true;
            }

            return false;
        }
    };

    bool init = false;
    std::optional<std::string> last_error = std::nullopt;

    rtc::Configuration rtc_config;
    std::unordered_map<NutBlast_PlayerID, std::vector<rtc::Candidate>> incoming_candidates;
    std::unordered_map<NutBlast_PlayerID, std::vector<rtc::Description>> incoming_offers;

    std::string our_gid = "";
    NutBlast_PlayerID our_pid = 0;
    NutBlast_LobbyID our_lid = 0;
    std::string server_address;
    ByeReason disconnection_reason = ByeReason::OK;
    int max_players = 0;

    NutBlast_ChannelID max_channels = 1;
    Locked<std::deque<Message>> recv_queues[MAX_CHANNELS];

    enum class Mode {
        Host,
        Join,
        List,
    } mode = Mode::Join;

    bool lobby_listed = true;
    Locked<bool> permission_to_cook = false, time_to_die = false;

    std::uint64_t our_birth = 0;
    std::size_t listing_limit = 0;

    std::unordered_map<NutBlast_PlayerID, std::shared_ptr<Player>> players;
    NutBlast_PlayerID master = 0;

    Pinger server_pinger;
    Once fire_ready;

    std::shared_ptr<rtc::WebSocket> websocket = nullptr;
    Locked<std::vector<nlohmann::json>> ws_in, ws_out;

    Metadata player_meta, lobby_meta;

    void (*logger)(NutBlast_LogLevel, const char*) = nullptr;
    NutBlast_LogLevel log_level = NB_LogInfo;

    void log_to_stdout(NutBlast_LogLevel level, const char* line) {
        std::fprintf(stdout, "NB[%s] %s\n", NutBlast_LogLevelToString(level), line);
        std::fflush(stdout);
    }

    template <const NutBlast_LogLevel level, typename... Args>
    inline void log(std::format_string<Args...> fmt, Args&&... args) {
        const auto log = (logger == nullptr ? log_to_stdout : logger);

        if constexpr (level == NB_LogError) {
            last_error = std::vformat(fmt.get(), std::make_format_args(args...));
            if (level >= log_level)
                log(level, last_error->c_str());
        } else if (level >= log_level) {
            const auto line = std::vformat(fmt.get(), std::make_format_args(args...));
            log(level, line.c_str());
        }
    }

    template <typename... Args> inline void info(std::format_string<Args...> fmt, Args&&... args) {
        log<NB_LogInfo>(fmt, std::forward<Args>(args)...);
    }

    template <typename... Args> inline void error(std::format_string<Args...> fmt, Args&&... args) {
        log<NB_LogError>(fmt, std::forward<Args>(args)...);
    }

    template <typename... Args> inline void trace(std::format_string<Args...> fmt, Args&&... args) {
        log<NB_LogTrace>(fmt, std::forward<Args>(args)...);
    }

    void send(const nlohmann::json& obj) {
        auto queue = ws_out.lock();
        queue->emplace_back(obj);

        if (websocket && websocket->isOpen()) {
            try {
                for (const auto& obj : copy_and_clear(*queue))
                    websocket->send(obj.dump());
            } catch (const std::runtime_error&) {
                *time_to_die.lock() = true;
                return;
            }
        }
    }

    bool check_field(const char* Type, const char* key, const char* value) {
        if (!key || !key[0] || std::strlen(key) > NUTBLAST_FIELD_NAME_MAX) {
            NutBlast::error("{} metadata: invalid key size", Type);
            return false;
        }

        if (value && std::strlen(value) > NUTBLAST_FIELD_VALUE_MAX) {
            NutBlast::error("{} metadata: invalid value size", Type);
            return false;
        }

        return true;
    }

    void set_metadata(const char* Type, const char* type, Metadata& meta, const char* key, const char* value) {
        if (value == nullptr) {
            meta.erase(key);

            send({
                {"type", std::string("Erase") + Type + "Meta"},
                {"key", key},
            });
        } else if (meta.size() >= NUTBLAST_MAX_FIELDS && !meta.contains(key)) {
            NutBlast::error("Reached {} {} fields limit", NUTBLAST_MAX_FIELDS, type);
        } else {
            meta.insert_or_assign(key, value);

            send({
                {"type", std::string("Set") + Type + "Meta"},
                {"key", key},
                {"value", value},
            });
        }
    }

    void join() {
        last_error = std::nullopt;
        our_pid = master = 0;

        ws_in.lock()->clear(), ws_out.lock()->clear();
        incoming_candidates.clear(), incoming_offers.clear();

        *permission_to_cook.lock() = false;
        disconnection_reason = ByeReason::OK;
        server_pinger.reset();

        for (auto& queue : recv_queues)
            queue.lock()->clear();

        websocket = std::make_shared<rtc::WebSocket>(
#ifndef __EMSCRIPTEN__
            rtc::WebSocketConfiguration{
#ifndef _WIN32
                .caCertificatePemFile = "/etc/ssl/certs/ca-certificates.crt",
#endif
            }
#endif
        );

        websocket->onOpen([]() {
            if (mode == Mode::List) {
                send({
                    {"type", "List"},
                    {"gid", our_gid},
                    {"limit", listing_limit},
                });
            } else if (mode == Mode::Host) {
                send({
                    {"type", "Host"},
                    {"gid", our_gid},
                    {"capacity", max_players},
                    {"listed", lobby_listed},
                    {"player_meta", player_meta},
                    {"lobby_meta", lobby_meta},
                });
            } else {
                send({
                    {"type", "Join"},
                    {"gid", our_gid},
                    {"lid", our_lid},
                    {"player_meta", player_meta},
                });
            }
        });

        websocket->onMessage([](const auto& msg) {
            if (!std::holds_alternative<rtc::string>(msg))
                return;

            try {
                auto obj = nlohmann::json::parse(std::get<std::string>(msg));
                ws_in.lock()->emplace_back(obj);
            } catch (const nlohmann::json::parse_error& err) {
                (void)err;
            }
        });

        websocket->onClosed([]() {
            *time_to_die.lock() = true;
        });

        websocket->open(server_address);
    }

#ifdef __EMSCRIPTEN__
    struct IdMember {
        typedef int rtc::PeerConnection::* type;
        friend type get(IdMember);
    };

    template <typename Tag, typename Tag::type M> struct Rob {
        friend typename Tag::type get(Tag) {
            return M;
        }
    };

    template struct Rob<IdMember, &rtc::PeerConnection::mId>;
#endif

    void handle_offer_or_answer(const nlohmann::json& obj) {
        if (NutBlast_IgnoreICE)
            return;

        const NutBlast_PlayerID pid = obj.at("from");
        const auto& type = obj.at("type") == "Offer" ? "offer" : "answer";

        if (!incoming_offers.contains(pid))
            incoming_offers.insert({pid, {}});

        incoming_offers.at(pid).emplace_back(obj.at("sdp"), type);
    }

    void handle_candidate(const nlohmann::json& obj) {
        if (NutBlast_IgnoreICE)
            return;

        const NutBlast_PlayerID pid = obj.at("from");

        if (!incoming_candidates.contains(pid))
            incoming_candidates.insert({pid, {}});

        try {
            auto& queue = incoming_candidates.at(pid);
            queue.emplace_back(obj.at("candidate"), obj.at("mid"));
        } catch (const std::invalid_argument&) {
            incoming_candidates.erase(pid);
        }
    }

    struct LobbyInfo {
        std::vector<std::pair<std::string, std::string>> fields;
        std::vector<NutBlast_LobbyField> meta;
    };

    void handle_list(const nlohmann::json& obj) {
        std::vector<NutBlast_Lobby> lobbies;
        std::vector<LobbyInfo> tmp;

        const auto& lobers = obj.at("list");
        tmp.reserve(lobers.size());

        for (const auto& lober : lobers) {
            tmp.push_back({});

            LobbyInfo& tlob = tmp.back();

            const auto& read_meta = lober.at("meta");
            tlob.fields.reserve(read_meta.size());

            for (const auto& [key, value] : read_meta.items()) {
                tlob.fields.push_back({key, value.get<std::string>()});

                tlob.meta.push_back({
                    .key = tlob.fields.back().first.c_str(),
                    .value = tlob.fields.back().second.c_str(),
                });
            }

            lobbies.push_back({
                .id = lober.at("lid"),
                .players = lober.at("players"),
                .capacity = lober.at("max"),
                .metadata = tlob.meta.data(),
                .field_count = tlob.meta.size(),
            });
        }

        on_lobbies_found(lobbies.data(), lobbies.size());
    }

    const std::unordered_map<std::string, void (*)(const nlohmann::json&)> response_handlers{
        {"Connected",
            [](const auto& obj) {
                rtc_config = rtc::Configuration();
#ifndef __EMSCRIPTEN__
                rtc_config.enableIceUdpMux = true;
                rtc_config.iceTransportPolicy = rtc::TransportPolicy::All; // rtc::TransportPolicy::Relay
#endif

                our_pid = obj.at("pid"), our_lid = obj.at("lid"), our_birth = obj.at("birth");
                NutBlast::info("You are ID={}", our_pid);

                NutBlast::info("ICE servers from NutBlaster:");

                for (const auto& obj : obj.at("ice_servers")) {
                    const std::string& hostname = obj.at("urls");
                    rtc::IceServer ice_server(hostname);

                    if (obj.contains("username")) {
                        ice_server.username = obj.at("username");
                        ice_server.password = obj.at("credential");
                    }

                    const std::string& kind = obj.at("kind");

                    if (kind == "Stun")
                        ice_server.type = rtc::IceServer::Type::Stun;
                    else
                        ice_server.type = rtc::IceServer::Type::Turn;

                    if (kind == "TurnUdp")
                        ice_server.relayType = rtc::IceServer::RelayType::TurnUdp;
                    else if (kind == "TurnTcp")
                        ice_server.relayType = rtc::IceServer::RelayType::TurnTcp;
                    else if (kind == "TurnTls")
                        ice_server.relayType = rtc::IceServer::RelayType::TurnTls;

                    NutBlast::info("  {} {}", kind == "Stun" ? "STUN" : "TURN", hostname);

                    rtc_config.iceServers.push_back(ice_server);
                }

                *permission_to_cook.lock() = true;
            }},
        {"Disconnected",
            [](const auto& obj) {
                disconnection_reason = obj.at("reason");
                *time_to_die.lock() = true;
            }},
        {"SetListed",
            [](const auto& obj) {
                lobby_listed = obj.at("listed");
            }},
        {"SetCapacity",
            [](const auto& obj) {
                max_players = obj.at("capacity");
            }},
        {"SetPlayerMeta",
            [](const auto& obj) {
                const NutBlast_PlayerID pid = obj.at("pid");

                if (!NutBlast_IsPlayerInLobby(pid) || pid == NutBlast_GetPlayerID())
                    return;

                const auto& player = players.at(pid);
                auto& meta = player->meta;

                const std::string key = obj.at("key"), new_value = obj.at("value");
                std::optional<std::string> old_value;

                if (meta.contains(key))
                    old_value = meta.at(key);

                if (old_value != new_value) {
                    meta.insert_or_assign(key, new_value);

                    NutBlast_FieldDiff diff = {0};
                    diff.name = key.c_str();
                    diff.old_value = old_value.has_value() ? old_value->c_str() : nullptr;
                    diff.new_value = new_value.c_str();

                    if (player->fire_join.fired())
                        on_player_meta_changed(pid, diff);
                }
            }},
        {"ErasePlayerMeta",
            [](const auto& obj) {
                const NutBlast_PlayerID pid = obj.at("pid");

                if (!NutBlast_IsPlayerInLobby(pid) || pid == NutBlast_GetPlayerID())
                    return;

                auto& meta = players.at(pid)->meta;
                const std::string key = obj.at("key");

                if (!meta.contains(key))
                    return;

                NutBlast_FieldDiff diff = {0};
                diff.name = key.c_str();
                diff.old_value = meta.at(key).c_str();
                diff.new_value = nullptr;

                on_player_meta_changed(pid, diff);
                meta.erase(key);
            }},
        {"SetLobbyMeta",
            [](const auto& obj) {
                const std::string key = obj.at("key"), new_value = obj.at("value");
                std::optional<std::string> old_value;

                if (lobby_meta.contains(key))
                    old_value = lobby_meta.at(key);

                if (old_value != new_value) {
                    lobby_meta.insert_or_assign(key, new_value);

                    NutBlast_FieldDiff diff = {0};
                    diff.name = key.c_str();
                    diff.old_value = old_value.has_value() ? old_value->c_str() : nullptr;
                    diff.new_value = new_value.c_str();

                    on_lobby_meta_changed(diff);
                }
            }},
        {"EraseLobbyMeta",
            [](const auto& obj) {
                const std::string key = obj.at("key");

                if (!lobby_meta.contains(key))
                    return;

                NutBlast_FieldDiff diff = {0};
                diff.name = key.c_str();
                diff.old_value = lobby_meta.at(key).c_str();
                diff.new_value = nullptr;

                on_lobby_meta_changed(diff);
                lobby_meta.erase(key);
            }},
        {"SetMaster",
            [](const auto& obj) {
                const auto old_master = master;
                master = obj.at("pid");

                if (old_master != master)
                    on_master_changed(old_master);
            }},
        {"Joined",
            [](const auto& obj) {
                const NutBlast_PlayerID id = obj.at("pid");
                players.insert({id, std::make_shared<Player>(id, obj.at("meta"), obj.at("birth"))});
            }},
        {"Left",
            [](const auto& obj) {
                const NutBlast_PlayerID pid = obj.at("pid");

                if (players.contains(pid)) {
                    on_player_left(pid, ByeReason(obj.at("reason")));
                    players.erase(pid);
                }
            }},
        {"Offer", handle_offer_or_answer},
        {"Answer", handle_offer_or_answer},
        {"Candidate", handle_candidate},
        {"List", handle_list},
        {"Pong",
            [](const auto& obj) {
                server_pinger.pong();

                const std::unordered_set<NutBlast_PlayerID> remaining_players = obj.at("players");

                std::erase_if(players, [&remaining_players](const auto& pair) {
                    const auto pid = pair.first;

                    if (remaining_players.contains(pid))
                        return false;

                    on_player_left(pid, ByeReason::OK);
                    return true;
                });
            }},
    };
} // namespace NutBlast

extern "C" const char* NutBlast_LogLevelToString(NutBlast_LogLevel level) {
    switch (level) {
    case NB_LogTrace:
        return "TRACE";
    case NB_LogInfo:
        return "INFO";
    case NB_LogError:
        return "ERROR";
    }

    return "";
}

extern "C" void NutBlast_Init(NutBlast_InitOptions opts) {
    if (NutBlast::init) {
        NutBlast::error("Don't call `NutBlast_Init()` twice!");
        return;
    }

    if (!opts.game_id) {
        NutBlast::error("`game_id` must be set!");
        return;
    }

    if (opts.max_channels > NutBlast::MAX_CHANNELS) {
        NutBlast::error("Expected up to {} channels, got {}", NutBlast::MAX_CHANNELS, opts.max_channels);
        return;
    }

    NutBlast::init = true;

    rtc::Preload();

    NutBlast::info(".-------------------------------------------------------------.");
    NutBlast::info("| For troubleshooting multiplayer connectivity, please visit: |");
    NutBlast::info("|    https://github.com/Schwungus/NutBlast#troubleshooting    |");
    NutBlast::info("'-------------------------------------------------------------'");

    if (NutBlast::server_address.empty())
        NutBlast::server_address = NUTBLAST_DEFAULT_SERVER;

    NutBlast_SetGameID(opts.game_id);
    NutBlast::max_channels = opts.max_channels ? opts.max_channels : 1;
}

extern "C" void NutBlast_SetGameID(const char* gid) {
    if (gid) {
        NutBlast::our_gid = gid;
        NutBlast::info("Playing \"{}\"", gid);
    } else {
        NutBlast::error("game ID cannot be null");
    }
}

extern "C" void NutBlast_SetNutBlasterAddress(const char* address) {
    NutBlast::server_address = address ? address : NUTBLAST_DEFAULT_SERVER;
}

extern "C" void NutBlast_SetMaxPlayers(int max) {
    if (max < 2 || max > NUTBLAST_MAX_PLAYERS)
        return;

    NutBlast::max_players = max;

    NutBlast::send({
        {"type", "SetCapacity"},
        {"capacity", NutBlast::max_players},
    });
}

extern "C" void NutBlast_SetListed(bool listed) {
    NutBlast::send({
        {"type", "SetListed"},
        {"listed", listed},
    });
}

extern "C" bool NutBlast_IsListed() {
    return NutBlast::lobby_listed;
}

extern "C" void NutBlast_Disconnect() {
    *NutBlast::time_to_die.lock() = false;

    if (NutBlast::websocket) {
        NutBlast::websocket->onOpen(nullptr);
        NutBlast::websocket->onClosed(nullptr);
        NutBlast::websocket->onMessage(nullptr);

        try {
            NutBlast::websocket->close();
        } catch (const std::runtime_error& err) {
            (void)err;
        }
    }

    for (auto& [id, player] : NutBlast::players) {
        if (!player)
            continue;

        if (player->unreliable_dc)
            player->unreliable_dc->close();

        if (player->reliable_dc)
            player->reliable_dc->close();

        if (player->ping_dc)
            player->ping_dc->close();

        if (player->pc)
            player->pc->close();
    }

    NutBlast::ws_in.lock()->clear();
    NutBlast::ws_out.lock()->clear();
    NutBlast::players.clear();

    NutBlast::incoming_candidates.clear();
    NutBlast::incoming_offers.clear();

    NutBlast::websocket = nullptr;
    NutBlast::fire_ready.reset();

    const auto reason = NutBlast::disconnection_reason;
    NutBlast::disconnection_reason = NutBlast::ByeReason::OK;

    NutBlast::info("NutBlaster out! {} ({})", reason.msg, reason.code);
    NutBlast::on_disconnected(reason); // TODO: maybe NOT fire this in the lobby-listing mode?

    NutBlast::our_birth = 0;
    NutBlast::listing_limit = 0;
    NutBlast::max_players = 0;

    NutBlast::our_pid = 0;
    NutBlast::master = 0;
    NutBlast::our_lid = 0;

    NutBlast::last_error = std::nullopt;
}

extern "C" void NutBlast_FindLobbies(size_t limit) {
    if (NutBlast_IsConnecting()) {
        NutBlast::error("You're already connected!");
    } else if (!NutBlast::init) {
        NutBlast::error("You forgot to call `NutBlast_Init()`");
    } else {
        NutBlast::mode = NutBlast::Mode::List;
        NutBlast::listing_limit = limit;

        NutBlast::info("Connecting to {}", NutBlast::server_address);
        NutBlast::join();
    }
}

extern "C" void NutBlast_Join(NutBlast_LobbyID id) {
    if (NutBlast_IsConnecting()) {
        NutBlast::error("You're already connected!");
    } else if (!NutBlast::init) {
        NutBlast::error("You forgot to call `NutBlast_Init()`");
    } else if (!id) {
        NutBlast::error("No lobby ID specified!");
    } else {
        NutBlast::mode = NutBlast::Mode::Join;
        NutBlast::our_lid = id;

        NutBlast::info("Trying to join '{}'", id);
        NutBlast::join();
    }
}

extern "C" void NutBlast_Host(NutBlast_HostOptions opts) {
    if (NutBlast_IsConnecting()) {
        NutBlast::error("You're already connected!");
    } else if (!NutBlast::init) {
        NutBlast::error("You forgot to call `NutBlast_Init()`");
    } else if (opts.max_players && (opts.max_players < 2 || opts.max_players > NUTBLAST_MAX_PLAYERS)) {
        NutBlast::error("Max player count specified but not in the range of 2-{}", NUTBLAST_MAX_PLAYERS);
    } else {
        NutBlast::mode = NutBlast::Mode::Host;
        NutBlast::lobby_listed = !opts.unlisted;
        NutBlast::max_players = opts.max_players ? opts.max_players : NUTBLAST_MAX_PLAYERS;

        NutBlast::info("Trying to host");
        NutBlast::join();
    }
}

extern "C" int NutBlast_GetPlayerCount() {
    return NutBlast_IsOnline() ? static_cast<int>(1 + NutBlast::players.size()) : 0;
}

extern "C" int NutBlast_GetMaxPlayers() {
    return NutBlast_IsOnline() ? NutBlast::max_players : 0;
}

extern "C" const NutBlast_PlayerID* NutBlast_ListPlayers() {
    static NutBlast_PlayerID buf[NUTBLAST_MAX_PLAYERS + 1] = {0};
    std::size_t i = 0;

    if (NutBlast_GetPlayerID())
        buf[i++] = NutBlast_GetPlayerID();

    for (const auto& [id, player] : NutBlast::players)
        buf[i++] = id;

    buf[i] = 0;

    return buf;
}

extern "C" NutBlast_PlayerID NutBlast_GetPlayerID() {
    return *NutBlast::permission_to_cook.lock() ? NutBlast::our_pid : 0;
}

extern "C" NutBlast_LobbyID NutBlast_GetLobbyID() {
    return NutBlast_IsOnline() ? NutBlast::our_lid : 0;
}

extern "C" NutBlast_PlayerID NutBlast_GetMasterID() {
    return NutBlast_IsOnline() ? NutBlast::master : 0;
}

extern "C" bool NutBlast_IsPlayerInLobby(NutBlast_PlayerID pid) {
    if (!pid || !NutBlast_IsOnline())
        return false;

    if (pid == NutBlast_GetPlayerID())
        return true;

    return NutBlast::players.contains(pid) && NutBlast::players.at(pid);
}

extern "C" bool NutBlast_IsConnectedToPlayer(NutBlast_PlayerID pid) {
    return NutBlast_IsPlayerInLobby(pid) && (pid == NutBlast_GetPlayerID() || NutBlast::players.at(pid)->is_ready());
}

extern "C" void NutBlast_Flush() {
    static NutBlast::Ticker beater(NutBlast::interval::beat), pinger(NutBlast::interval::ping);

    if (!NutBlast_IsOnline() || NutBlast::mode == NutBlast::Mode::List)
        return;

    if (pinger) {
        NutBlast::server_pinger.ping();

        for (auto& [id, player] : NutBlast::players) {
            const auto dc = player->ping_dc.get();

            if (dc && dc->isOpen()) {
                player->pinger.ping();
                dc->send("PING");
            }
        }

        NutBlast::send({
            {"type", "Ping"},
        });
    }

    if (beater) {
        for (auto& [id, player] : NutBlast::players) {
            auto lock = player->outgoing_candidates.lock();

            for (const auto& candidate : NutBlast::copy_and_clear(*lock)) {
                NutBlast::send({
                    {"type", "PassCandidate"},
                    {"to", id},
                    {"candidate", (rtc::string)candidate},
                    {"mid", candidate.mid()},
                });
            }
        }
    }
}

extern "C" void NutBlast_Update() {
    for (const auto& obj : NutBlast::copy_and_clear(*NutBlast::ws_in.lock())) {
        try {
            const std::string& type = obj.at("type");
            const auto handler = NutBlast::response_handlers.find(type);

            if (handler != NutBlast::response_handlers.end())
                handler->second(obj);
        } catch (const nlohmann::json::out_of_range& e) {
            NutBlast::error("Communication error caused by a version mismatch between the NutBlast client library and "
                            "the server: {}",
                e.what());
        }
    }

    for (auto& [id, player] : NutBlast::players)
        player->drain_incoming_offers_and_candidates();

    if (*NutBlast::time_to_die.lock()) {
        NutBlast_Disconnect();
        return;
    }

    if (*NutBlast::permission_to_cook.lock()) {
        for (auto& [id, player] : NutBlast::players) {
            if (player->unreliable_dc != nullptr && player->unreliable_dc->isOpen()) {
                player->sdp_timeout = std::nullopt;
                player->renegotiation_attempts = 0;
            }

            if (player->sdp_timeout.has_value() && NutBlast_TimeNS() >= *player->sdp_timeout) {
                // attempt to renegotiate a connection up to `MAX_RENEGOTIATION_ATTEMPTS` times. then disconnect to let
                // others play in peace.
                player->renegotiation_attempts += 1;
                player->init.reset();

                if (player->renegotiation_attempts > NutBlast::MAX_RENEGOTIATION_ATTEMPTS) {
                    // only the newest player should disconnect. had to introduce this check because BOTH players kept
                    // disconnecting each time :wilted_flower:
                    if (NutBlast::our_birth >= player->birth) {
                        const std::string code = NUTBLAST_ERROR_STUN_FAILED;
                        const auto msg = std::format("Could not establish a P2P connection with player {}", id);

                        NutBlast::send({
                            {"type", "Leave"},
                            {"code", code},
                            {"msg", msg},
                        });

                        NutBlast::disconnection_reason = NutBlast::ByeReason(code, msg);
                        NutBlast_Disconnect();

                        return;
                    }
                } else {
                    NutBlast::info("Renegotiating with {} ({}/{})", player->pid, player->renegotiation_attempts,
                        NutBlast::MAX_RENEGOTIATION_ATTEMPTS);
                }
            }

            player->engage();
        }
    }

    if (NutBlast_IsReady()) {
        if (NutBlast::fire_ready && NutBlast::mode != NutBlast::Mode::List) {
            NutBlast::info("NutBlast connected and ready!");
            NutBlast::on_ready();
        }

        for (const auto& [id, player] : NutBlast::players) {
            if (player->fire_join) {
                NutBlast::on_player_joined(id);

                for (const auto& [key, value] : player->meta) {
                    NutBlast_FieldDiff diff = {0};
                    diff.name = key.c_str();
                    diff.old_value = nullptr;
                    diff.new_value = value.c_str();
                    NutBlast::on_player_meta_changed(id, diff);
                }
            }
        }
    }

    NutBlast_Flush();
}

extern "C" uint64_t NutBlast_TimeNS() {
    const auto elapsed = std::chrono::high_resolution_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
}

extern "C" void NutBlast_SetLogger(void (*cb)(NutBlast_LogLevel, const char*)) {
    NutBlast::logger = cb;
}

extern "C" void NutBlast_SetLogLevel(NutBlast_LogLevel level) {
    NutBlast::log_level = level;
}

extern "C" const char* NutBlast_GetLastError() {
    return NutBlast::last_error.has_value() ? NutBlast::last_error->c_str() : nullptr;
}

extern "C" void NutBlast_Kick(NutBlast_PlayerID guy) {
    if (guy != 0 && guy != NutBlast_GetPlayerID()) {
        NutBlast::send({
            {"type", "Kick"},
            {"pid", guy},
        });
    }
}

extern "C" void NutBlast_SetMaster(NutBlast_PlayerID guy) {
    if (guy != 0 && guy != NutBlast_GetPlayerID()) {
        NutBlast::send({
            {"type", "SetMaster"},
            {"pid", guy},
        });
    }
}

extern "C" void NutBlast_Send(NutBlast_SendOptions opts) {
    if (!opts.data) {
        NutBlast::error("Cannot send a null message");
        return;
    }

    if (opts.to == NutBlast_GetPlayerID() || !NutBlast_IsConnectedToPlayer(opts.to))
        return;

    const auto& player = NutBlast::players.at(opts.to);
    const auto& dc = opts.reliable ? player->reliable_dc : player->unreliable_dc;

    if (dc == nullptr)
        return;

    if (!opts.size)
        opts.size = std::strlen(reinterpret_cast<const char*>(opts.data)) + 1;

    rtc::binary buf(1 + opts.size);
    buf[0] = rtc::byte(opts.channel);

    for (std::size_t i = 0; i < opts.size; i++)
        buf[i + 1] = rtc::byte(reinterpret_cast<const std::uint8_t*>(opts.data)[i]);

    try {
        dc->send(buf);
    } catch (const std::runtime_error& err) {
        (void)err;
    }
}

extern "C" bool NutBlast_NextMessage(NutBlast_ChannelID chan, NutBlast_Message* out) {
    if (!out) {
        NutBlast::error("NutBlast_NextMessage called with null pointer");
        return false;
    }

    if (chan >= NutBlast::max_channels) {
        NutBlast::error(
            "NutBlast_NextMessage called with channel {} out of {} max channels", chan, NutBlast::max_channels);
        return false;
    }

    auto queue = NutBlast::recv_queues[chan].lock();

    if (queue->empty())
        return false;

    static NutBlast::Message msg; // keeps the buffers valid between calls
    msg = std::move(queue->front());
    queue->pop_front();

    out->from = msg.from;
    out->data = (const char*)msg.bytes.data();
    out->size = msg.bytes.size();

    return true;
}

extern "C" bool NutBlast_IsConnecting() {
    return NutBlast::websocket != nullptr;
}

extern "C" bool NutBlast_IsOnline() {
    return NutBlast::websocket && NutBlast::websocket->isOpen();
}

extern "C" bool NutBlast_IsReady() {
    if (!NutBlast_IsOnline() || !*NutBlast::permission_to_cook.lock())
        return false;

    for (const auto& [id, player] : NutBlast::players)
        if (!player->is_ready())
            return false;

    return true;
}

extern "C" bool NutBlast_IsPlayerConnectionRelayed(NutBlast_PlayerID pid) {
    if (!NutBlast_IsPlayerInLobby(pid) || pid == NutBlast_GetPlayerID())
        return false;

    const auto& player = NutBlast::players.at(pid);

#ifdef __EMSCRIPTEN__
    const int id = (*player->pc).*get(NutBlast::IdMember());
    const auto js = emscripten::val::global("WEBRTC")["peerConnectionsMap"][id];

    if (js.isUndefined() || js.isNull())
        return false;

    const auto pair = js["__selectedCandidatePair"];

    if (pair.isUndefined() || pair.isNull())
        return false;

    const auto local_type = pair["localType"].as<std::string>();
    const auto remote_type = pair["remoteType"].as<std::string>();

    return local_type == "relay" || remote_type == "relay";
#else
    rtc::Candidate local_c, remote_c;

    if (!player->pc->getSelectedCandidatePair(&local_c, &remote_c))
        return false;

    const auto local = local_c.candidate();
    const auto remote = remote_c.candidate();

    const bool local_is_turn = local.find("typ relay") != std::string::npos;
    const bool remote_is_turn = remote.find("typ relay") != std::string::npos;

    return local_is_turn || remote_is_turn;
#endif
}

extern "C" int NutBlast_ServerPing() {
    return NutBlast_IsOnline() ? NutBlast::server_pinger.millis() : 0;
}

extern "C" int NutBlast_PlayerPing(NutBlast_PlayerID pid) {
    if (NutBlast_IsPlayerInLobby(pid) && pid != NutBlast_GetPlayerID()) {
        const auto& player = NutBlast::players.at(pid);
        return player->is_ready() ? player->pinger.millis() : 0;
    } else {
        return 0;
    }
}

extern "C" void NutBlast_SleepMS(int _ms) {
#ifdef _WIN32
    Sleep(_ms);
#else
    time_t ms = _ms;

    // Stolen from: <https://stackoverflow.com/a/1157217>
    struct timespec ts = {0};
    ts.tv_sec = ms / 1000, ts.tv_nsec = (ms % 1000) * (time_t)NutBlast::ns::milli;
    int res = 0;

    do {
        res = nanosleep(&ts, &ts);
    } while (res && errno == EINTR);
#endif
}

extern "C" void NutBlast_Cleanup() {
    NutBlast_Disconnect();
    rtc::Cleanup().wait();
}

extern "C" const char* NutBlast_GetPlayerField(NutBlast_PlayerID pid, const char* name) {
    if (!name)
        return nullptr;

    // could be 0 == 0 btw: `NutBlast_GetPlayerID()` returns 0 until connected to a lobby
    if (pid == NutBlast_GetPlayerID())
        return NutBlast::player_meta.contains(name) ? NutBlast::player_meta.at(name).c_str() : nullptr;

    if (!NutBlast_IsPlayerInLobby(pid))
        return nullptr;

    const auto& player = NutBlast::players.at(pid);
    return player->meta.contains(name) ? player->meta.at(name).c_str() : nullptr;
}

extern "C" void NutBlast_SetPlayerField(const char* key, const char* value) {
    if (NutBlast::check_field("Player", key, value))
        NutBlast::set_metadata("Player", "player", NutBlast::player_meta, key, value);
}

extern "C" const char* NutBlast_GetLobbyField(const char* name) {
    if (!name || !NutBlast_IsOnline())
        return nullptr;

    if (NutBlast::lobby_meta.contains(name))
        return NutBlast::lobby_meta.at(name).c_str();

    return nullptr;
}

extern "C" void NutBlast_SetLobbyField(const char* key, const char* value) {
    const auto master = NutBlast_GetMasterID();

    if (master != NutBlast_GetPlayerID())
        return;

    if (NutBlast::check_field("Lobby", key, value))
        NutBlast::set_metadata("Lobby", "lobby", NutBlast::lobby_meta, key, value);
}

extern "C" void NutBlast_PurgeMetadata() {
    NutBlast::player_meta.clear();
    NutBlast::lobby_meta.clear();
}

NutBlast::Player::~Player() {
    if (NutBlast::incoming_offers.contains(pid))
        NutBlast::incoming_offers.erase(pid);

    if (NutBlast::incoming_candidates.contains(pid))
        NutBlast::incoming_candidates.erase(pid);
}

void NutBlast::Player::drain_incoming_offers_and_candidates() {
    if (!pc)
        return;

    if (incoming_offers.contains(pid)) {
        for (const auto& offer : copy_and_clear(incoming_offers.at(pid))) {
            trace("offer/answer from {}: {}", pid, static_cast<std::string>(offer));

            try {
                pc->setRemoteDescription(offer);
                trace("offer/answer accepted!");
            } catch (...) {
                continue;
            }
        }
    }

    if (pc->remoteDescription().has_value() && incoming_candidates.contains(pid)) {
        for (const auto& candidate : copy_and_clear(incoming_candidates.at(pid))) {
            trace("candidate from {}: {}", pid, static_cast<std::string>(candidate));

            try {
                pc->addRemoteCandidate(candidate);
                trace("candidate accepted!");
            } catch (...) {
                continue;
            }
        }
    }
}

void NutBlast::Player::engage() {
    if (!init)
        return;

    const auto id = this->pid;
    sdp_timeout = NutBlast_TimeNS() + interval::sdp_timeout;

    pc = std::make_shared<rtc::PeerConnection>(rtc_config);

    pc->onLocalDescription([=](const auto& desc) {
        std::string type;

        if (desc.typeString() == "offer")
            type = "PassOffer";
        else if (desc.typeString() == "answer")
            type = "PassAnswer";
        else
            return;

        send({
            {"type", type},
            {"to", id},
            {"sdp", (rtc::string)desc},
        });
    });

    pc->onLocalCandidate([self = weak_from_this()](const auto& candidate) {
        if (self.expired())
            return;

        auto state = self.lock();
        state->outgoing_candidates.lock()->emplace_back(candidate);
    });

    const auto on_msg = [=](const auto& variant) {
        if (!std::holds_alternative<rtc::binary>(variant))
            return;

        const auto& bytes = std::get<rtc::binary>(variant);

        if (bytes.empty())
            return;

        const auto chan = static_cast<NutBlast_ChannelID>(bytes[0]);

        if (chan >= max_channels)
            return;

        rtc::binary buf(bytes.begin() + 1, bytes.end());
        recv_queues[chan].lock()->emplace_back(id, std::move(buf));
    };

    const auto on_ping = [=, self = weak_from_this()](const auto& msg) {
        if (self.expired() || !std::holds_alternative<rtc::string>(msg))
            return;

        const auto state = self.lock();

        if (state->ping_dc == nullptr)
            return;

        const auto type = std::get<rtc::string>(msg);

        if (type == "PING")
            state->ping_dc->send("PONG");
        else if (type == "PONG")
            state->pinger.pong();
    };

    if (is_offerer()) {
        unreliable_dc = pc->createDataChannel("unreliable", {
            .reliability = { .unordered = true, .maxRetransmits = 0, },
        });

        unreliable_dc->onMessage(on_msg);

        reliable_dc = pc->createDataChannel("reliable", {
	        // don't repeat my mistake MIGUEL > If both maxPacketLifeTime or maxRetransmits are unset, the channel is reliable.
            .reliability = { .unordered = false, },
        });

        reliable_dc->onMessage(on_msg);

        ping_dc = pc->createDataChannel("ping", {
            .reliability = { .unordered = true, .maxRetransmits = 0, },
        });

        ping_dc->onMessage(on_ping);
    } else {
        pc->onDataChannel([=, self = weak_from_this()](const auto& dc) {
            if (self.expired())
                return;

            const auto state = self.lock();

            if (dc->label() == "reliable" || dc->label() == "unreliable") {
                const bool reliable = (dc->label() == "reliable");

                (reliable ? state->reliable_dc : state->unreliable_dc) = dc;
                dc->onMessage(on_msg);
            } else {
                state->ping_dc = dc;
                dc->onMessage(on_ping);
            }
        });
    }
}
