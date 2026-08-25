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
#include <optional>
#include <random>
#include <string>
#include <unordered_map>

#include <rtc/rtc.hpp>
#include <rtc/websocket.hpp>

#include <nlohmann/json.hpp>

#include <NutBlast.h>

static constexpr const bool WINDOSE =
#ifdef _WIN32
    true;
#include <windows.h>
#else
    false;
#include <errno.h>
#endif

static constexpr const size_t MAX_CHANNELS = 16;

using Metadata = std::unordered_map<std::string, std::string>;

namespace ns {
    constexpr const std::uint64_t second = 1000000000, milli = second / 1000;
};

namespace interval {
    constexpr const std::uint64_t beat = ::ns::second / 62, ping = ::ns::second;
};

template <typename T> static T copy_and_clear(T& obj) {
    auto copy = obj;
    obj.clear();
    return copy;
}

extern "C" const char* NutBlast_LogLevelToString(NutBlast_LogLevel level) {
    switch (level) {
    case NB_LogInfo:
        return "INFO";
    case NB_LogError:
        return "ERROR";
    }

    return "";
}

static void (*logger)(NutBlast_LogLevel, const char*) = nullptr;

void NutBlast_SetLogger(void (*cb)(NutBlast_LogLevel, const char*)) {
    ::logger = cb;
}

static void log_to_stdout(NutBlast_LogLevel level, const char* line) {
    std::fprintf(stdout, "NB[%s] %s\n", NutBlast_LogLevelToString(level), line);
    std::fflush(stdout);
}

template <typename... Args>
static inline void log(NutBlast_LogLevel level, std::format_string<Args...> fmt, Args&&... args) {
    const auto logger = ::logger == nullptr ? log_to_stdout : ::logger;
    logger(level, std::vformat(fmt.get(), std::make_format_args(args...)).c_str());
}

extern "C" uint64_t NutBlast_TimeNS() {
    const auto elapsed = std::chrono::high_resolution_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
}

static rtc::Configuration rtc_config;
static std::unordered_map<NutBlast_ID, std::vector<rtc::Candidate>> incoming_candidates;
static std::unordered_map<NutBlast_ID, std::vector<rtc::Description>> incoming_offers;

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
            last_roundtrip = (NutBlast_TimeNS() - last_ping) / (2 * ::ns::milli);
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
    bool err = false;
    std::string code = NUTBLAST_ERROR_OK, msg = "Graceful disconnection";

    ByeReason() {}
    ByeReason(const nlohmann::json& obj) : err(obj["type"] == "violation"), code(obj["code"]), msg(obj["msg"]) {}

    operator NutBlast_Reason() const {
        return {.err = err, .code = code.c_str(), .msg = msg.c_str()};
    }
};

struct Player : std::enable_shared_from_this<Player> {
    const NutBlast_ID pid;
    Once fire_join, init_once;

    Pinger pinger;
    Metadata meta;

    std::shared_ptr<rtc::PeerConnection> pc = nullptr;
    std::shared_ptr<rtc::DataChannel> reliable_dc = nullptr, unreliable_dc = nullptr, ping_dc = nullptr;

    std::vector<rtc::Candidate> outgoing_candidates;
    std::mutex outgoing_candidates_mutex;

    Player(NutBlast_ID pid, const Metadata& meta) : pid(pid), meta(meta) {}

    ~Player() {
        if (::incoming_offers.contains(pid))
            ::incoming_offers.erase(pid);

        if (::incoming_candidates.contains(pid))
            ::incoming_candidates.erase(pid);
    }

    void init();

    bool is_offerer() const {
        return NutBlast_GetOurID() > pid;
    }

    bool is_online() const {
        return unreliable_dc && reliable_dc && ping_dc;
    }

    void drain_incoming_offers_and_candidates() {
        if (!pc)
            return;

        if (::incoming_offers.contains(pid)) {
            for (const auto& offer : copy_and_clear(::incoming_offers.at(pid))) {
                try {
                    pc->setRemoteDescription(offer);
                } catch (...) { continue; }
            }
        }

        if (pc->remoteDescription().has_value() && ::incoming_candidates.contains(pid)) {
            for (const auto& candidate : copy_and_clear(::incoming_candidates.at(pid))) {
                try {
                    pc->addRemoteCandidate(candidate);
                } catch (...) { continue; }
            }
        }
    }
};

struct Message {
    NutBlast_ID from;
    std::vector<std::uint8_t> bytes;

    Message() = default;
    Message(NutBlast_ID from, const std::vector<std::uint8_t>& bytes) : from(from), bytes(bytes) {}
};

static std::string gid = "";
static NutBlast_ID pid = 0, lid = 0;
static std::optional<std::string> blaster;
static ByeReason disconnection_reason;

static std::mutex globals_mutex;

static NutBlast_ChannelID max_chan = 1;
static struct {
    std::mutex mutex;
    std::deque<Message> messages;
} recv_queues[MAX_CHANNELS];

static enum class Mode {
    Host,
    Join,
    List,
    Swarm,
} mode = Mode::Join;

static bool hosting_a_listed_lobby = true, permission_to_cook = false, time_to_die = false;
static std::size_t listing_limit = 0;

static std::unordered_map<NutBlast_ID, std::shared_ptr<Player>> players;
static NutBlast_ID master = 0;

static Pinger ws_pinger;
static Once fire_ready;
static std::shared_ptr<rtc::WebSocket> blaster_ws = nullptr;
static std::vector<nlohmann::json> ws_in, ws_out;

static Metadata player_meta, lobby_meta;

// TODO: refactor each of these into a struct.
static void (*on_ready)() = nullptr, (*on_disconnected)(NutBlast_Reason) = nullptr,
            (*on_player_joined)(NutBlast_ID) = nullptr, (*on_player_left)(NutBlast_ID, NutBlast_Reason) = nullptr,
            (*on_lobbies_found)(const NutBlast_Lobby*, size_t) = nullptr, (*on_master_changed)(NutBlast_ID) = nullptr,
            (*on_player_meta_changed)(NutBlast_ID, NutBlast_FieldDiff) = nullptr,
            (*on_lobby_meta_changed)(NutBlast_FieldDiff) = nullptr;

template <typename... Args> static void fire(void (*cb)(Args...), const std::decay_t<Args>&... args) {
    if (cb != nullptr)
        cb(args...);
}

static void ws_send(const nlohmann::json& obj) {
    ::ws_out.emplace_back(obj);

    if (!NutBlast_IsOnline())
        return;

    try {
        for (const auto& obj : copy_and_clear(::ws_out))
            ::blaster_ws->send(obj.dump());
    } catch (const std::runtime_error&) {
        ::time_to_die = true;
        return;
    }
}

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

void Player::init() {
    if (!init_once)
        return;

    const auto id = this->pid;

    pc = std::make_shared<rtc::PeerConnection>(::rtc_config);

    pc->onLocalDescription([=](const auto& desc) {
        std::string type;

        if (desc.typeString() == "offer")
            type = "PassOffer";
        else if (desc.typeString() == "answer")
            type = "PassAnswer";
        else
            return;

        ::ws_send({
            {"type", type},
            {"to", id},
            {"sdp", (rtc::string)desc},
        });
    });

    pc->onLocalCandidate([self = weak_from_this()](const auto& candidate) {
        if (self.expired())
            return;

        auto state = self.lock();
        std::lock_guard<std::mutex> lock(state->outgoing_candidates_mutex);
        state->outgoing_candidates.emplace_back(candidate);
    });

    const auto on_msg = [=](const auto& variant) {
        if (!std::holds_alternative<rtc::binary>(variant))
            return;

        const auto& bytes = std::get<rtc::binary>(variant);

        if (bytes.empty())
            return;

        const auto chan = static_cast<NutBlast_ChannelID>(bytes[0]);

        if (chan >= ::max_chan)
            return;

        auto& queue = ::recv_queues[chan];

        std::lock_guard<std::mutex> lock(queue.mutex);
        std::vector<std::uint8_t> buf(bytes.begin() + 1, bytes.end());
        queue.messages.emplace_back(id, std::move(buf));
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
            .reliability = {.unordered = true, .maxRetransmits = 0, },
        });

        unreliable_dc->onMessage(on_msg);

        reliable_dc = pc->createDataChannel("reliable", {
            .reliability = {.unordered = false, },
        });

        reliable_dc->onMessage(on_msg);

        ping_dc = pc->createDataChannel("ping", {
            .reliability = {.unordered = true, .maxRetransmits = 0, },
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

static NutBlast_ID generate_id() {
    static constexpr const char characters[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    static std::mt19937 mt{std::random_device()()};
    std::uniform_int_distribution<size_t> dist(0, sizeof(characters) - 2);

    char id[sizeof(NutBlast_ID)];

    for (char& c : id)
        c = characters[dist(mt)];

    return *reinterpret_cast<NutBlast_ID*>(id);
}

static std::string get_blaster() {
    if (blaster == std::nullopt) {
        log(NB_LogInfo, "Using the default NutBlaster server as none was explicitly specified: {}",
            NUTBLAST_DEFAULT_SERVER);
        blaster = NUTBLAST_DEFAULT_SERVER;
    }

    return *blaster;
}

static int max_players = NUTBLAST_MAX_PLAYERS;

extern "C" void NutBlast_SetNutBlaster(const char* blaster) {
    ::blaster = (blaster == nullptr ? NUTBLAST_DEFAULT_SERVER : blaster);
}

extern "C" void NutBlast_SetGameID(const char* gid) {
    ::gid = (gid == nullptr ? "" : gid);
}

extern "C" void NutBlast_SetMaxChannels(NutBlast_ChannelID max) {
    if (max > 0 && max <= MAX_CHANNELS)
        ::max_chan = max;
}

extern "C" void NutBlast_SetMaxPlayers(int max) {
    if (max > 1 && max <= NUTBLAST_MAX_PLAYERS)
        ::max_players = max;
    else
        return;

    ::ws_send({
        {"type", "SetCapacity"},
        {"capacity", ::max_players},
    });
}

extern "C" void NutBlast_SetListed(bool listed) {
    ::hosting_a_listed_lobby = listed;

    ::ws_send({
        {"type", "SetListed"},
        {"listed", listed},
    });
}

extern "C" bool NutBlast_IsListed() {
    return ::hosting_a_listed_lobby;
}

extern "C" const char* NutBlast_GetPlayerField(NutBlast_ID pid, const char* name) {
    if (!pid || !name)
        return nullptr;

    if (pid == NutBlast_GetOurID())
        return ::player_meta.contains(name) ? ::player_meta.at(name).c_str() : nullptr;

    if (!NutBlast_IsOnline())
        return nullptr;

    if (!::players.contains(pid))
        return nullptr;

    const auto& player = ::players.at(pid);
    return player->meta.contains(name) ? player->meta.at(name).c_str() : nullptr;
}

static bool check_field(const char* type, const char* key, const char* value) {
    if (!key || !key[0] || std::strlen(key) > NUTBLAST_FIELD_NAME_MAX) {
        log(NB_LogError, "{} metadata: invalid key size", type);
        return false;
    }

    if (value && std::strlen(value) > NUTBLAST_FIELD_VALUE_MAX) {
        log(NB_LogError, "{} metadata: invalid value size", type);
        return false;
    }

    return true;
}

static void
freak_metadata(const char* type, const char* type_lower, Metadata& meta, const char* key, const char* value) {
    if (value == nullptr) {
        meta.erase(key);

        ::ws_send({
            {"type", std::string("Erase") + type + "Meta"},
            {"key", key},
        });
    } else if (meta.size() >= NUTBLAST_MAX_FIELDS && !meta.contains(key)) {
        log(NB_LogError, "Reached {} {} fields limit", NUTBLAST_MAX_FIELDS, type_lower);
    } else {
        meta.insert_or_assign(key, value);

        ::ws_send({
            {"type", std::string("Set") + type + "Meta"},
            {"key", key},
            {"value", value},
        });
    }
}

extern "C" void NutBlast_SetPlayerField(const char* key, const char* value) {
    if (check_field("Player", key, value))
        freak_metadata("Player", "player", ::player_meta, key, value);
}

extern "C" const char* NutBlast_GetLobbyField(const char* name) {
    if (!name || !NutBlast_IsOnline())
        return nullptr;

    if (::lobby_meta.contains(name))
        return ::lobby_meta.at(name).c_str();

    return nullptr;
}

extern "C" void NutBlast_SetLobbyField(const char* key, const char* value) {
    if (!check_field("Lobby", key, value))
        return;

    if (NutBlast_IsOnline()) {
        const auto master = NutBlast_GetMasterID();

        if (!master || master != NutBlast_GetOurID())
            return;
    }

    freak_metadata("Lobby", "lobby", ::lobby_meta, key, value);
}

extern "C" void NutBlast_PurgeMetadata() {
    ::player_meta.clear(), ::lobby_meta.clear();
}

static void join_pro() {
    std::lock_guard<std::mutex> lock(::globals_mutex);

    rtc::Preload();
    ::ws_in.clear(), ::ws_out.clear();
    ::incoming_candidates.clear(), ::incoming_offers.clear();

    get_blaster();
    ::master = 0, ::disconnection_reason = ByeReason(), ::permission_to_cook = false;
    ::ws_pinger.reset();

    for (auto& queue : recv_queues) {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.messages.clear();
    }

#ifndef __EMSCRIPTEN__
    rtc::WebSocketConfiguration conf;

    if constexpr (!WINDOSE)
        conf.caCertificatePemFile = "/etc/ssl/certs/ca-certificates.crt";
#endif

    ::blaster_ws = std::make_shared<rtc::WebSocket>(
#ifndef __EMSCRIPTEN__
        conf
#endif
    );

    ::blaster_ws->onOpen([]() {
        if (::mode == Mode::List) {
            ::ws_send({
                {"type", "List"},
                {"gid", ::gid},
                {"limit", ::listing_limit},
            });
        } else if (::mode == Mode::Swarm) {
            ::ws_send({
                {"type", "Swarm"},
                {"gid", ::gid},
                {"pid", NutBlast_GetOurID()},
                {"player_meta", ::player_meta},
                {"lobby_meta", ::lobby_meta},
            });
        } else if (::mode == Mode::Host) {
            ::ws_send({
                {"type", "Host"},
                {"gid", ::gid},
                {"pid", NutBlast_GetOurID()},
                {"lid", ::lid},
                {"capacity", ::max_players},
                {"listed", ::hosting_a_listed_lobby},
                {"player_meta", ::player_meta},
                {"lobby_meta", ::lobby_meta},
            });
        } else {
            ::ws_send({
                {"type", "Join"},
                {"gid", ::gid},
                {"pid", NutBlast_GetOurID()},
                {"lid", ::lid},
                {"player_meta", ::player_meta},
            });
        }
    });

    ::blaster_ws->onMessage([](const auto& msg) {
        if (!std::holds_alternative<rtc::string>(msg))
            return;

        try {
            auto obj = nlohmann::json::parse(std::get<std::string>(msg));
            std::lock_guard<std::mutex> lock(::globals_mutex);
            ::ws_in.emplace_back(obj);
        } catch (const nlohmann::json::parse_error&) {}
    });

    ::blaster_ws->onClosed([]() {
        ::time_to_die = true;
    });

    ::blaster_ws->open(get_blaster());
}

extern "C" void NutBlast_Disconnect() {
    ::time_to_die = false;

    if (::blaster_ws) {
        ::blaster_ws->onClosed(nullptr);
        ::blaster_ws->onMessage(nullptr);

        try {
            ::blaster_ws->close();
        } catch (const std::runtime_error&) {}
    }

    for (auto& [id, player] : ::players)
        if (player && player->pc)
            player->pc->close();

    {
        std::lock_guard<std::mutex> lock(::globals_mutex);

        ::ws_in.clear(), ::ws_out.clear(), ::players.clear();
        ::incoming_candidates.clear(), ::incoming_offers.clear();
        ::blaster_ws = nullptr, ::lid = 0;
        ::fire_ready.reset();
    }

    log(NB_LogInfo, "NutBlaster out! ({})", ::disconnection_reason.msg);

    // TODO: maybe NOT fire this in the lobby-listing mode?
    fire(::on_disconnected, ::disconnection_reason);

    ::disconnection_reason = ByeReason();
}

extern "C" void NutBlast_FindLobbies(size_t limit) {
    if (::blaster_ws) {
        log(NB_LogError, "You're already connected!");
    } else {
        ::mode = Mode::List, ::listing_limit = limit;
        log(NB_LogInfo, "Connecting to {}", get_blaster());
        join_pro();
    }
}

extern "C" void NutBlast_Join(NutBlast_ID id) {
    if (::blaster_ws) {
        log(NB_LogError, "You're already connected!");
    } else if (!id) {
        log(NB_LogError, "No ID specified!");
    } else {
        ::mode = Mode::Join, ::lid = id;
        log(NB_LogInfo, "Trying to join '{}' at: {}", id, get_blaster());
        join_pro();
    }
}

extern "C" void NutBlast_Host(NutBlast_ID id, int max, bool listed) {
    if (::blaster_ws) {
        log(NB_LogError, "You're already connected!");
    } else {
        NutBlast_SetMaxPlayers(max);
        ::mode = Mode::Host, ::hosting_a_listed_lobby = listed;
        ::lid = id ? id : generate_id();

        log(NB_LogInfo, "Trying to host '{}' at: {}", lid, get_blaster());
        join_pro();
    }
}

extern "C" void NutBlast_JoinSwarm() {
    if (::blaster_ws) {
        log(NB_LogError, "You're already connected!");
    } else {
        ::mode = Mode::Swarm;
        log(NB_LogInfo, "Trying to join a swarm for '{}'", ::gid);
        join_pro();
    }
}

extern "C" int NutBlast_GetPlayerCount() {
    return NutBlast_IsOnline() ? static_cast<int>(1 + players.size()) : 0;
}

extern "C" int NutBlast_GetMaxPlayers() {
    return NutBlast_IsOnline() ? ::max_players : 0;
}

extern "C" const NutBlast_ID* NutBlast_GetPlayerIDs() {
    static NutBlast_ID buf[NUTBLAST_MAX_PLAYERS + 1] = {0};
    size_t i = 0;

    buf[i++] = NutBlast_GetOurID();

    for (const auto& [id, player] : players)
        buf[i++] = id;

    buf[i] = 0;

    return buf;
}

extern "C" NutBlast_ID NutBlast_GetOurID() {
    if (!pid) {
        pid = generate_id();
        log(NB_LogInfo, "You are {}", pid);
    }

    return pid;
}

extern "C" NutBlast_ID NutBlast_GetLobbyID() {
    return (NutBlast_IsOnline() && ::lid) ? ::lid : 0;
}

extern "C" NutBlast_ID NutBlast_GetMasterID() {
    return NutBlast_IsOnline() ? ::master : 0;
}

extern "C" bool NutBlast_IsPlayerAlive(NutBlast_ID pid) {
    if (!pid || !NutBlast_IsOnline())
        return false;

    if (pid == NutBlast_GetOurID())
        return true;

    return ::players.contains(pid);
}

static void handle_offer_or_answer(const nlohmann::json& obj) {
    const NutBlast_ID pid = obj["from"];
    const auto& type = obj["type"] == "Offer" ? "offer" : "answer";

    if (!::incoming_offers.contains(pid))
        ::incoming_offers.insert({pid, {}});

    ::incoming_offers.at(pid).emplace_back(obj["sdp"], type);
}

static void handle_candidate(const nlohmann::json& obj) {
    const NutBlast_ID& pid = obj["from"];

    if (!::incoming_candidates.contains(pid))
        ::incoming_candidates.insert({pid, {}});

    try {
        auto& queue = ::incoming_candidates.at(pid);
        queue.emplace_back(obj["candidate"], obj["mid"]);
    } catch (const std::invalid_argument&) { ::incoming_candidates.erase(pid); }
}

struct LobbyInfo {
    std::vector<std::pair<std::string, std::string>> fields;
    std::vector<NutBlast_LobbyField> meta;
};

static void handle_list(const nlohmann::json& obj) {
    std::vector<NutBlast_Lobby> lobbies;
    std::vector<LobbyInfo> tmp;

    const auto& lobers = obj["list"];
    tmp.reserve(lobers.size());

    for (const auto& lober : lobers) {
        tmp.push_back({});

        LobbyInfo& tlob = tmp.back();

        const auto& read_meta = lober["meta"];
        tlob.fields.reserve(read_meta.size());

        for (const auto& [key, value] : read_meta.items()) {
            tlob.fields.push_back({key, value.get<std::string>()});

            tlob.meta.push_back({
                .key = tlob.fields.back().first.c_str(),
                .value = tlob.fields.back().second.c_str(),
            });
        }

        lobbies.push_back({
            .id = lober["lid"],
            .players = lober["players"],
            .capacity = lober["max"],
            .metadata = tlob.meta.data(),
            .field_count = tlob.meta.size(),
        });
    }

    fire(::on_lobbies_found, lobbies.data(), lobbies.size());
}

static const std::unordered_map<std::string, void (*)(const nlohmann::json&)> response_types{
    {"Connected",
        [](const auto& obj) {
            ::rtc_config.iceServers.clear();

            log(NB_LogInfo, "ICE servers from NutBlaster:");

            for (const auto& server : obj["ice_servers"]) {
                ::rtc_config.iceServers.emplace_back(server);
                log(NB_LogInfo, "  {}", (std::string)server);
            }

            ::permission_to_cook = true;
        }},
    {"Disconnected",
        [](const auto& obj) {
            ::disconnection_reason = obj["reason"];
            ::time_to_die = true;
        }},
    {"SetListed",
        [](const auto& obj) {
            ::hosting_a_listed_lobby = obj["listed"];
        }},
    {"SetCapacity",
        [](const auto& obj) {
            ::max_players = obj["capacity"];
        }},
    {"SetPlayerMeta",
        [](const auto& obj) {
            const NutBlast_ID pid = obj["pid"];

            if (!::players.contains(pid))
                return;

            const auto& player = ::players.at(pid);
            auto& meta = player->meta;

            const std::string key = obj["key"], new_value = obj["value"];
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
                    fire(::on_player_meta_changed, pid, diff);
            }
        }},
    {"ErasePlayerMeta",
        [](const auto& obj) {
            const NutBlast_ID pid = obj["pid"];

            if (!::players.contains(pid))
                return;

            auto& meta = ::players.at(pid)->meta;
            const std::string key = obj["key"];

            if (!meta.contains(key))
                return;

            NutBlast_FieldDiff diff = {0};
            diff.name = key.c_str();
            diff.old_value = meta.at(key).c_str();
            diff.new_value = nullptr;

            fire(::on_player_meta_changed, pid, diff);
            meta.erase(key);
        }},
    {"SetLobbyMeta",
        [](const auto& obj) {
            const std::string key = obj["key"], new_value = obj["value"];
            std::optional<std::string> old_value;

            if (::lobby_meta.contains(key))
                old_value = ::lobby_meta.at(key);

            if (old_value != new_value) {
                ::lobby_meta.insert_or_assign(key, new_value);

                NutBlast_FieldDiff diff = {0};
                diff.name = key.c_str();
                diff.old_value = old_value.has_value() ? old_value->c_str() : nullptr;
                diff.new_value = new_value.c_str();

                fire(::on_lobby_meta_changed, diff);
            }
        }},
    {"EraseLobbyMeta",
        [](const auto& obj) {
            const std::string key = obj["key"];

            if (!::lobby_meta.contains(key))
                return;

            NutBlast_FieldDiff diff = {0};
            diff.name = key.c_str();
            diff.old_value = ::lobby_meta.at(key).c_str();
            diff.new_value = nullptr;

            fire(::on_lobby_meta_changed, diff);
            ::lobby_meta.erase(key);
        }},
    {"SetMaster",
        [](const auto& obj) {
            const auto old_master = ::master;
            ::master = obj["pid"];

            if (old_master != ::master)
                fire(::on_master_changed, old_master);
        }},
    {"Joined",
        [](const auto& obj) {
            const NutBlast_ID id = obj["pid"];
            ::players.insert({id, std::make_shared<Player>(id, obj["meta"])});
        }},
    {"Left",
        [](const auto& obj) {
            const NutBlast_ID pid = obj["pid"];

            if (::players.contains(pid)) {
                const bool got_reason = obj.contains("reason") && !obj["reason"].is_null();
                fire(::on_player_left, pid, got_reason ? obj["reason"] : ByeReason());
                ::players.erase(pid);
            }
        }},
    {"Offer", handle_offer_or_answer},
    {"Answer", handle_offer_or_answer},
    {"Candidate", handle_candidate},
    {"List", handle_list},
    {"Pong",
        [](const auto&) {
            ::ws_pinger.pong();
        }},
};

static void recv_stuff() {
    std::lock_guard<std::mutex> lock(::globals_mutex);

    for (const auto& obj : copy_and_clear(::ws_in)) {
        if (!obj.contains("type"))
            continue;

        const auto restype = response_types.find(obj["type"]);

        if (restype != response_types.end())
            restype->second(obj);
    }

    for (auto& [id, player] : ::players)
        player->drain_incoming_offers_and_candidates();
}

extern "C" void NutBlast_Flush() {
    static Ticker beater(interval::beat), pinger(interval::ping);

    if (!NutBlast_IsOnline() || ::mode == Mode::List)
        return;

    if (pinger) {
        ::ws_pinger.ping();

        for (auto& [id, player] : ::players) {
            const auto dc = player->ping_dc.get();

            if (dc && dc->isOpen()) {
                player->pinger.ping();
                dc->send("PING");
            }
        }

        ::ws_send({
            {"type", "Ping"},
        });
    }

    if (beater) {
        for (auto& [id, player] : ::players) {
            std::lock_guard<std::mutex> lock(player->outgoing_candidates_mutex);

            for (const auto& candidate : copy_and_clear(player->outgoing_candidates)) {
                ::ws_send({
                    {"type", "PassCandidate"},
                    {"to", id},
                    {"candidate", (rtc::string)candidate},
                    {"mid", candidate.mid()},
                });
            }
        }
    }
}

static void init_players_after_ready() {
    if (::fire_ready && ::mode != Mode::List) {
        log(NB_LogInfo, "NutBlast connected and ready!");
        fire(::on_ready);
    }

    for (const auto& [id, player] : ::players) {
        if (player->fire_join) {
            fire(::on_player_joined, id);

            for (const auto& [key, value] : player->meta) {
                NutBlast_FieldDiff diff = {0};
                diff.name = key.c_str();
                diff.old_value = nullptr;
                diff.new_value = value.c_str();
                fire(::on_player_meta_changed, pid, diff);
            }
        }
    }
}

extern "C" void NutBlast_Update() {
    recv_stuff();

    if (::time_to_die) {
        NutBlast_Disconnect();
        return;
    }

    if (::permission_to_cook)
        for (auto& [id, player] : ::players)
            player->init();

    if (NutBlast_IsReady())
        init_players_after_ready();

    NutBlast_Flush();
}

extern "C" void NutBlast_Kick(NutBlast_ID guy) {
    ::ws_send({
        {"type", "Kick"},
        {"pid", guy},
    });
}

extern "C" void NutBlast_SetMaster(NutBlast_ID guy) {
    ::ws_send({
        {"type", "SetMaster"},
        {"pid", guy},
    });
}

static void greatest_technician_thats_ever_lived(
    NutBlast_ChannelID chan, NutBlast_ID pid, const char* msg, int size, bool reliable) {
    if (!::players.contains(pid))
        return;

    if (!msg) {
        log(NB_LogError, "Cannot send a null message");
        return;
    }

    const auto& player = ::players.at(pid);

    if (size < 0)
        size = (int)std::strlen(msg) + 1;

    rtc::binary buf(1 + size);

    buf[0] = chan;

    for (size_t i = 0; i < size; i++)
        buf[i + 1] = msg[i];

    try {
        const auto& dc = reliable ? player->reliable_dc : player->unreliable_dc;

        if (dc)
            dc->send(buf);
    } catch (const std::runtime_error&) {}
}

extern "C" void NutBlast_SendTo(NutBlast_ChannelID chan, NutBlast_ID id, const char* msg, int size) {
    greatest_technician_thats_ever_lived(chan, id, msg, size, false);
}

extern "C" void NutBlast_SendReliablyTo(NutBlast_ChannelID chan, NutBlast_ID id, const char* msg, int size) {
    greatest_technician_thats_ever_lived(chan, id, msg, size, true);
}

extern "C" bool NutBlast_NextMessage(NutBlast_ChannelID chan, NutBlast_Message* out) {
    if (!out) {
        log(NB_LogError, "NutBlast_NextMessage called with null pointer");
        return false;
    }

    if (chan >= ::max_chan) {
        log(NB_LogError, "NutBlast_NextMessage called with channel {} out of {} max channels", chan, ::max_chan);
        return false;
    }

    auto& queue = ::recv_queues[chan];
    std::lock_guard<std::mutex> lock(queue.mutex);

    if (queue.messages.empty())
        return false;

    static Message msg; // keeps the buffers valid between calls
    msg = std::move(queue.messages.front());
    queue.messages.pop_front();

    out->from = msg.from;
    out->data = (const char*)msg.bytes.data();
    out->size = msg.bytes.size();

    return true;
}

extern "C" bool NutBlast_IsOnline() {
    return ::blaster_ws && ::blaster_ws->isOpen();
}

extern "C" bool NutBlast_IsReady() {
    if (!NutBlast_IsOnline() || !::permission_to_cook)
        return false;

    for (const auto& [id, player] : ::players)
        if (!player->is_online())
            return false;

    return true;
}

#define MakeCb(name, var, ...)                                                                                         \
    extern "C" void NutBlast_##name(void (*cb)(__VA_ARGS__)) {                                                         \
        (var) = cb;                                                                                                    \
    }

MakeCb(OnReady, ::on_ready);
MakeCb(OnDisconnected, ::on_disconnected, NutBlast_Reason);
MakeCb(OnPlayerJoined, ::on_player_joined, NutBlast_ID);
MakeCb(OnPlayerLeft, ::on_player_left, NutBlast_ID, NutBlast_Reason);
MakeCb(OnLobbiesFound, ::on_lobbies_found, const NutBlast_Lobby*, size_t);
MakeCb(OnMasterChanged, ::on_master_changed, NutBlast_ID);
MakeCb(OnPlayerMetadataChanged, ::on_player_meta_changed, NutBlast_ID, NutBlast_FieldDiff);
MakeCb(OnLobbyMetadataChanged, ::on_lobby_meta_changed, NutBlast_FieldDiff);

extern "C" int NutBlast_ServerPing() {
    return NutBlast_IsOnline() ? ::ws_pinger.millis() : 0;
}

extern "C" int NutBlast_PlayerPing(NutBlast_ID pid) {
    if (!::players.contains(pid))
        return 0;

    const auto& player = ::players.at(pid);
    return player->is_online() ? player->pinger.millis() : 0;
}

extern "C" void NutBlast_SleepMS(int _ms) {
#ifdef _WIN32
    Sleep(_ms);
#else
    time_t ms = _ms;

    // Stolen from: <https://stackoverflow.com/a/1157217>
    struct timespec ts = {0};
    ts.tv_sec = ms / 1000, ts.tv_nsec = (ms % 1000) * (time_t)ns::milli;
    int res = 0;
    do { res = nanosleep(&ts, &ts); } while (res && errno == EINTR);
#endif
}

extern "C" void NutBlast_Cleanup() {
    NutBlast_Disconnect();
    rtc::Cleanup().wait();
}
