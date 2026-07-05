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

#include <array>
#include <chrono>
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

using Metadata = std::unordered_map<std::string, std::string>;

namespace ns {
    constexpr const std::uint64_t second = 1000000000, milli = second / 1000;
};

namespace interval {
    constexpr const std::uint64_t beat = ::ns::second / 62, ping = ::ns::second;
};

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

static const rtc::Configuration rtc_config = {
    .iceServers = {
        {"stun:stun.l.google.com:19302"},
        {"stun:stun1.l.google.com:19302"},
        {"stun:stun2.l.google.com:19302"},
        {"stun:stun3.l.google.com:19302"},
        {"stun:stun4.l.google.com:19302"},
    },
};

static std::unordered_map<NutBlast_ID, std::vector<rtc::Candidate>> incoming_candidates;
static std::unordered_map<NutBlast_ID, std::vector<rtc::Description>> incoming_offers;

template <typename V> static std::vector<V> copy_and_clear(std::vector<V>& vec) {
    const auto copy = vec;
    vec.clear();
    return copy;
}

struct Pinger {
    std::uint64_t last_ping = 0, last_roundtrip = 0;

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

struct ByeReason {
    bool err = false;
    std::string code = "ok", msg = "Graceful Disconnection";

    ByeReason() {}
    ByeReason(const nlohmann::json& obj) : err(obj["err"]), code(obj["code"]), msg(obj["msg"]) {}

    operator NutBlast_Reason() const {
        return {.err = err, .code = code.c_str(), .msg = msg.c_str()};
    }
};

struct Player {
    const NutBlast_ID id;
    bool joined = false;

    Pinger pinger;
    Metadata meta;

    std::shared_ptr<rtc::PeerConnection> pc = nullptr;
    std::shared_ptr<rtc::DataChannel> reliable_dc = nullptr, unreliable_dc = nullptr, ping_dc = nullptr;
    std::vector<rtc::Candidate> outgoing_candidates;

    Player(NutBlast_ID id, const Metadata& meta) : id(id), meta(meta) {}

    void init(const std::weak_ptr<Player>&);

    bool is_offerer() const;

    bool is_alive() const {
        return unreliable_dc && reliable_dc && ping_dc;
    }

    void drain_incoming_offers_and_candidates() {
        if (!::incoming_offers.contains(id))
            return;

        for (const auto& offer : copy_and_clear(::incoming_offers.at(id))) {
            try {
                pc->setRemoteDescription(offer);
            } catch (const std::invalid_argument&) { continue; }
        }

        if (!::incoming_candidates.contains(id))
            return;

        const auto copy = ::incoming_candidates.at(id);

        for (const auto& candidate : copy) {
            try {
                pc->addRemoteCandidate(candidate);
            } catch (const std::logic_error&) {
                return; // cannot use `copy_and_clear` here since that would clear incoming candidates immediately. we
                        // need them kept despite any state-related errors.
            }
        }

        ::incoming_candidates.at(id).clear();
    }
};

struct Message {
    NutBlast_ID from;
    std::vector<std::byte> bytes;
};

static std::string gid = "";
static NutBlast_ID pid = 0, lid = 0;
static std::optional<std::string> blaster;
static ByeReason disconnection_reason;

static NutBlast_ChannelID max_chan = 1;
static std::array<std::vector<Message>, 1 << 8 * sizeof(max_chan)> recv_queues;

static bool hosting = false, listing_lobbies = false, hosting_listed_lobby = true;
static size_t listing_limit = 0;
static NutBlast_ID master = 0;

static std::unordered_map<NutBlast_ID, std::shared_ptr<Player>> players;

static Pinger ws_pinger;
static std::shared_ptr<rtc::WebSocket> blaster_ws = nullptr;
static std::vector<nlohmann::json> ws_in, ws_out;

static Metadata player_meta, lobby_meta;

// TODO: refactor each of these into a struct.
static void (*on_connected)() = nullptr, (*on_disconnected)(NutBlast_Reason) = nullptr,
            (*on_player_joined)(NutBlast_ID) = nullptr, (*on_player_left)(NutBlast_ID, NutBlast_Reason) = nullptr,
            (*on_lobbies_found)(const NutBlast_Lobby*, size_t) = nullptr, (*on_master_changed)(NutBlast_ID) = nullptr,
            (*on_player_meta_changed)(NutBlast_ID, NutBlast_FieldDiff) = nullptr,
            (*on_lobby_meta_changed)(NutBlast_FieldDiff) = nullptr;

template <typename... Args> static void fire(void (*cb)(Args...), const std::decay_t<Args>&... args) {
    if (cb != nullptr)
        cb(args...);
}

static bool ws_ready() {
    return ::blaster_ws && ::blaster_ws->isOpen();
}

static void ws_send(nlohmann::json&& obj) {
    ws_out.push_back(obj);

    if (!ws_ready())
        return;

    try {
        for (const auto& obj : copy_and_clear(ws_out))
            ::blaster_ws->send(obj.dump());
    } catch (const std::runtime_error&) { NutBlast_Disconnect(); }
}

struct Ticker {
    const std::uint64_t interval;
    std::uint64_t last_tick = 0;

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

// gotta use a weak `this` pointer in lambda captures instead of plain `this` since we'll be sharing this `Player`
// instance with other threads with possibly different lifetimes, procured by libdatachannel in the background.
void Player::init(const std::weak_ptr<Player>& self) {
    const auto id = this->id;

    pc = std::make_shared<rtc::PeerConnection>(::rtc_config);

    pc->onLocalDescription([id](const auto& desc) {
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

    pc->onLocalCandidate([self](const auto& candidate) {
        if (self.expired())
            return;

        self.lock()->outgoing_candidates.push_back(candidate);
    });

    const auto on_msg = [id](const auto& variant) {
        if (!std::holds_alternative<rtc::binary>(variant))
            return;

        auto bytes = std::get<rtc::binary>(variant);

        if (bytes.empty())
            return;

        const auto chan = static_cast<NutBlast_ChannelID>(bytes[0]);

        if (chan < ::max_chan) {
            bytes.erase(bytes.begin());
            recv_queues[chan].push_back({.from = id, .bytes = bytes});
        }
    };

    const auto on_ping = [self](const auto& msg) {
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
        pc->onDataChannel([id, self, on_msg, on_ping](const auto& dc) {
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
    std::mt19937 mt;
    mt.seed(NutBlast_TimeNS());

    std::uniform_int_distribution<> dtype(0, 1), dalpha('A', 'Z'), ddigit('0', '9');
    static char id[sizeof(NutBlast_ID)] = "";

    for (size_t i = 0; i < sizeof(NutBlast_ID); i++)
        id[i] = static_cast<char>(dtype(mt) ? ddigit(mt) : dalpha(mt));

    return *reinterpret_cast<NutBlast_ID*>(id);
}

static NutBlast_ID get_pid() {
    if (!pid) {
        pid = generate_id();
        log(NB_LogInfo, "You are {}", pid);
    }

    return pid;
}

bool Player::is_offerer() const {
    return get_pid() > id;
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
    if (max)
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
    ::hosting_listed_lobby = listed;

    ::ws_send({
        {"type", "SetListed"},
        {"listed", listed},
    });
}

extern "C" bool NutBlast_IsListed() {
    return ::hosting_listed_lobby;
}

extern "C" const char* NutBlast_GetPlayerField(NutBlast_ID pid, const char* name) {
    if (!pid || !name)
        return nullptr;

    if (pid == get_pid())
        return ::player_meta.contains(name) ? ::player_meta.at(name).c_str() : nullptr;

    if (!NutBlast_IsReady())
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
    if (!name || !NutBlast_IsReady())
        return nullptr;

    if (::lobby_meta.contains(name))
        return ::lobby_meta.at(name).c_str();

    return nullptr;
}

extern "C" void NutBlast_SetLobbyField(const char* key, const char* value) {
    if (!check_field("Lobby", key, value))
        return;

    if (NutBlast_IsReady()) {
        const auto master = NutBlast_GetMasterID();

        if (!master || master != NutBlast_GetOurID())
            return;
    }

    freak_metadata("Lobby", "lobby", ::lobby_meta, key, value);
}

extern "C" void NutBlast_PurgeMetadata() {
    ::player_meta.clear(), ::lobby_meta.clear();
}

static void recv_stuff();

static void join_pro() {
    rtc::Preload();

    get_blaster();
    ::master = 0, ::disconnection_reason = ByeReason();
    ::ws_in.clear(), ::ws_out.clear();
    ::incoming_candidates.clear(), ::incoming_offers.clear();
    ::ws_pinger.reset();

    for (auto& queue : recv_queues)
        queue.clear();

#ifndef __EMSCRIPTEN__
    std::optional<rtc::string> ca = std::nullopt;

    if (!WINDOSE)
        ca = "/etc/ssl/certs/ca-certificates.crt";
#endif

    ::blaster_ws = std::make_shared<rtc::WebSocket>(
#ifndef __EMSCRIPTEN__
        rtc::WebSocketConfiguration{
            .caCertificatePemFile = ca,
        }
#endif
    );

    ::blaster_ws->onOpen([]() {
        if (listing_lobbies) {
            ::ws_send({
                {"type", "List"},
                {"gid", ::gid},
                {"limit", ::listing_limit},
            });
        } else {
            ::ws_send({
                {"type", "Connect"},
                {"mode", ::hosting ? "Host" : "Join"},
                {"gid", ::gid},
                {"pid", ::get_pid()},
                {"lid", ::lid},
                {"capacity", ::max_players},
                {"listed", ::hosting_listed_lobby},
                {"player_meta", ::player_meta},
                {"lobby_meta", ::lobby_meta},
            });

            fire(::on_connected);
        }
    });

    ::blaster_ws->onMessage([](const auto& msg) {
        if (!std::holds_alternative<rtc::string>(msg))
            return;

        try {
            ws_in.push_back(nlohmann::json::parse(std::get<std::string>(msg)));
        } catch (const nlohmann::json::parse_error&) {}
    });

    ::blaster_ws->onClosed(NutBlast_Disconnect);

    ::blaster_ws->open(get_blaster());
}

extern "C" void NutBlast_Disconnect() {
    if (::blaster_ws) {
        ::blaster_ws->onClosed(nullptr);

        recv_stuff();

        log(NB_LogInfo, "NutBlaster out!");
        fire(::on_disconnected, ::disconnection_reason);
    }

    if (::blaster_ws) { // `recv_stuff` could've nuked the socket so we check for null once again
        try {
            ::blaster_ws->close();
        } catch (const std::runtime_error&) {}
    }

    ::blaster_ws = nullptr, ::lid = 0;
    ::players.clear(), ::ws_in.clear(), ::ws_out.clear();
}

extern "C" void NutBlast_FindLobbies(size_t limit) {
    if (::blaster_ws) {
        log(NB_LogError, "You're already connected!");
    } else {
        ::listing_lobbies = true;
        ::listing_limit = limit;

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
        ::hosting = false, ::listing_lobbies = false;
        ::lid = id;

        log(NB_LogInfo, "Trying to join '{}' at: {}", id, get_blaster());
        join_pro();
    }
}

extern "C" void NutBlast_Host(NutBlast_ID id, int max, bool listed) {
    if (::blaster_ws) {
        log(NB_LogError, "You're already connected!");
    } else {
        NutBlast_SetMaxPlayers(max);
        ::hosting = true, ::listing_lobbies = false, ::hosting_listed_lobby = listed;
        ::lid = id ? id : generate_id();

        log(NB_LogInfo, "Trying to host '{}' at: {}", lid, get_blaster());
        join_pro();
    }
}

extern "C" int NutBlast_GetPlayerCount() {
    return NutBlast_IsReady() ? static_cast<int>(1 + players.size()) : 0;
}

extern "C" int NutBlast_GetMaxPlayers() {
    return NutBlast_IsReady() ? ::max_players : 0;
}

extern "C" const NutBlast_ID* NutBlast_GetPlayerIDs() {
    static NutBlast_ID buf[NUTBLAST_MAX_PLAYERS + 1] = {0};
    size_t i = 0;

    buf[i++] = get_pid();

    for (const auto& [id, player] : players)
        if (NutBlast_IsPlayerAlive(id))
            buf[i++] = id;

    buf[i] = 0;

    return buf;
}

extern "C" NutBlast_ID NutBlast_GetOurID() {
    return get_pid();
}

extern "C" NutBlast_ID NutBlast_GetLobbyID() {
    return (NutBlast_IsReady() && ::lid) ? ::lid : 0;
}

extern "C" NutBlast_ID NutBlast_GetMasterID() {
    return NutBlast_IsReady() ? ::master : 0;
}

extern "C" bool NutBlast_IsPlayerAlive(NutBlast_ID id) {
    if (!id || !NutBlast_IsReady())
        return false;

    if (id == get_pid())
        return true;

    if (!::players.contains(id))
        return false;

    return players.at(id)->is_alive();
}

static void handle_offer_or_answer(const nlohmann::json& obj) {
    const NutBlast_ID& id = obj["from"];

    if (!::incoming_offers.contains(id))
        ::incoming_offers.insert({id, {}});

    auto& queue = incoming_offers.at(id);

    const auto type = obj["type"] == "Offer" ? "offer" : "answer";
    queue.emplace_back(obj["sdp"], type);
}

static void handle_candidate(const nlohmann::json& obj) {
    const NutBlast_ID& id = obj["from"];

    if (!::incoming_candidates.contains(id))
        ::incoming_candidates.insert({id, {}});

    auto& queue = ::incoming_candidates.at(id);
    queue.emplace_back(obj["candidate"], obj["mid"]);
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

static const std::unordered_map<std::string, void (*)(const nlohmann::json&)> payload_types{
    {"Bye",
        [](const auto& obj) {
            ::disconnection_reason = obj["reason"];
            NutBlast_Disconnect();
        }},
    {"SetListed",
        [](const auto& obj) {
            ::hosting_listed_lobby = obj["listed"];
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

                if (player->joined)
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

            const auto ptr = std::make_shared<Player>(id, obj["meta"]);
            ::players.insert({id, ptr});
            ptr->init(ptr);
        }},
    {"Left",
        [](const auto& obj) {
            const NutBlast_ID pid = obj["pid"];

            if (::players.contains(pid)) {
                fire(::on_player_left, pid,
                    obj.contains("reason") && !obj["reason"].is_null() ? obj["reason"] : ByeReason());
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
    for (const auto& obj : copy_and_clear(::ws_in)) {
        if (!obj.contains("type"))
            continue;

        const std::string type = obj["type"];

        if (!payload_types.contains(type))
            continue;

        payload_types.at(type)(obj);
    }

    for (auto& [id, player] : ::players)
        player->drain_incoming_offers_and_candidates();
}

extern "C" void NutBlast_Flush() {
    static Ticker beater(interval::beat), pinger(interval::ping);

    if (!ws_ready() || listing_lobbies)
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

static void maybe_init_players_after_ready() {
    if (!NutBlast_IsReady())
        return;

    for (const auto& [id, player] : ::players) {
        if (player->joined)
            continue;

        player->joined = true;
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

extern "C" void NutBlast_Update() {
    recv_stuff();
    maybe_init_players_after_ready();
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
    NutBlast_ChannelID chan, NutBlast_ID id, const char* msg, int size, bool reliable) {
    if (!NutBlast_IsPlayerAlive(id))
        return; // TODO: not fail quietly?

    if (!msg) {
        log(NB_LogError, "Cannot send a null message");
        return;
    }

    if (id == get_pid()) {
        log(NB_LogError, "Cannot send a message to yourself");
        return;
    }

    const auto& player = ::players.at(id);

    if (size < 0)
        size = (int)std::strlen(msg) + 1;

    rtc::binary buf(1 + size);
    buf[0] = static_cast<std::byte>(chan);

    for (size_t i = 0; i < size; i++)
        buf[i + 1] = static_cast<std::byte>(msg[i]);

    try {
        const auto& dc = reliable ? player->reliable_dc : player->unreliable_dc;
        dc && dc->send(buf);
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
        log(NB_LogError, "NutBlast_NextMessage wants a non-null pointer instead");
        return false;
    }

    auto& queue = recv_queues[chan];

    if (queue.empty())
        return false;

    static Message msg; // keeps the buffers valid between calls

    msg = queue.front();
    queue.erase(queue.begin());

    out->data = reinterpret_cast<const char*>(msg.bytes.data());
    out->size = msg.bytes.size();
    out->from = msg.from;

    return true;
}

extern "C" bool NutBlast_IsReady() {
    if (!ws_ready())
        return false;

    for (const auto& [id, player] : ::players)
        if (!player->is_alive())
            return false;

    return true;
}

#define MakeCb(name, var, ...)                                                                                         \
    extern "C" void NutBlast_##name(void (*cb)(__VA_ARGS__)) {                                                         \
        (var) = cb;                                                                                                    \
    }

MakeCb(OnConnected, ::on_connected);
MakeCb(OnDisconnected, ::on_disconnected, NutBlast_Reason);
MakeCb(OnPlayerJoined, ::on_player_joined, NutBlast_ID);
MakeCb(OnPlayerLeft, ::on_player_left, NutBlast_ID, NutBlast_Reason);
MakeCb(OnLobbiesFound, ::on_lobbies_found, const NutBlast_Lobby*, size_t);
MakeCb(OnMasterChanged, ::on_master_changed, NutBlast_ID);
MakeCb(OnPlayerMetadataChanged, ::on_player_meta_changed, NutBlast_ID, NutBlast_FieldDiff);
MakeCb(OnLobbyMetadataChanged, ::on_lobby_meta_changed, NutBlast_FieldDiff);

extern "C" int NutBlast_ServerPing() {
    return NutBlast_IsReady() ? ::ws_pinger.millis() : 0;
}

extern "C" int NutBlast_PlayerPing(NutBlast_ID id) {
    if (!NutBlast_IsReady() || !::players.contains(id))
        return 0;
    return players.at(id)->pinger.millis();
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
