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
#include <ctime>
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

static void (*on_connected)() = nullptr, (*on_disconnected)(const char*) = nullptr, (*on_player_joined)(NutBlast_ID),
            (*on_player_left)(NutBlast_ID), (*on_lobbies_found)(const NutBlast_Lobby*, size_t);

template <typename... Args> static void fire(void (*cb)(Args...), Args... args) {
    if (cb != nullptr)
        cb(args...);
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

struct PeerSharedState {
    const NutBlast_ID id;

    std::shared_ptr<rtc::PeerConnection> pc = nullptr;
    std::shared_ptr<rtc::DataChannel> reliable_dc = nullptr, unreliable_dc = nullptr;
    std::vector<rtc::Candidate> outgoing_candidates;

    PeerSharedState(NutBlast_ID id) : id(id) {}

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

struct Peer {
    Metadata meta;
    std::shared_ptr<PeerSharedState> state;
    const NutBlast_ID id;

    Peer(NutBlast_ID id) : Peer(id, {}) {}
    Peer(NutBlast_ID, const Metadata&);

    bool is_offerer() const;

    bool is_alive() const {
        return state && state->unreliable_dc && state->unreliable_dc->isOpen() && state->reliable_dc
               && state->reliable_dc->isOpen();
    }
};

struct Message {
    NutBlast_ID from;
    std::vector<std::byte> bytes;
};

static std::string gid = "";
static NutBlast_ID pid = 0, lid = 0;
static std::optional<std::string> blaster, disconnection_reason, lname;

static NutBlast_ChannelID max_chan = 1;
static std::array<std::vector<Message>, 1 << 8 * sizeof(max_chan)> recv_queues;

static bool hosting = false, listing = false;
static NutBlast_ID master = 0;

static std::unordered_map<NutBlast_ID, Peer> peers;

static std::shared_ptr<rtc::WebSocket> blaster_ws = nullptr;
static std::vector<nlohmann::json> ws_in, ws_out;

static Metadata peer_meta, lobby_meta;

namespace ns {
    constexpr const std::uint64_t second = 1000000000, milli = second / 1000;
};

namespace interval {
    constexpr const std::uint64_t beat = ::ns::second / 62;
};

template <typename... Args> static inline void info(std::format_string<Args...> fmt, Args&&... args) {
    std::fprintf(stdout, "%s\n", std::vformat(fmt.get(), std::make_format_args(args...)).c_str());
    std::fflush(stdout);
}

extern "C" uint64_t NutBlast_TimeNS() {
    struct timespec ts = {0};
    timespec_get(&ts, TIME_UTC);
    return (std::uint64_t)ts.tv_sec * ns::second + (std::uint64_t)ts.tv_nsec;
}

static void ws_send(nlohmann::json&& obj) {
    ws_out.push_back(obj);

    if (!NutBlast_IsReady())
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

Peer::Peer(NutBlast_ID id, const Metadata& meta) : state(new PeerSharedState(id)), id(id), meta(meta) {
    const std::weak_ptr<PeerSharedState> st = state;

    state->pc = std::make_shared<rtc::PeerConnection>(::rtc_config);

    state->pc->onLocalDescription([id, st](const auto& local_desc) {
        if (st.expired())
            return;

        std::string type;

        if (local_desc.typeString() == "offer")
            type = "PassOffer";
        else if (local_desc.typeString() == "answer")
            type = "PassAnswer";
        else
            return;

        ::ws_send({
            {"type", type},
            {"to", id},
            {"sdp", (rtc::string)local_desc},
        });
    });

    state->pc->onLocalCandidate([st](const auto& candidate) {
        if (st.expired())
            return;

        st.lock()->outgoing_candidates.push_back(candidate);
    });

    const auto setup_dc = [id](rtc::DataChannel& dc) {
        dc.onOpen([id]() {
            fire(::on_player_joined, id);
        });

        dc.onClosed([id]() {
            fire(::on_player_left, id);
        });

        dc.onMessage([id](const auto& variant) {
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
        });
    };

    if (is_offerer()) {
        state->unreliable_dc = state->pc->createDataChannel("unreliable", {
            .reliability = {.unordered = true, .maxRetransmits = 0, },
        });

        setup_dc(*state->unreliable_dc);

        state->reliable_dc = state->pc->createDataChannel("reliable", {
            .reliability = {.unordered = false, },
        });

        setup_dc(*state->reliable_dc);
    } else {
        state->pc->onDataChannel([id, st, setup_dc](const auto& dc) {
            if (st.expired())
                return;

            const auto state = st.lock();

            if (dc->label() == "reliable")
                state->reliable_dc = dc;
            else
                state->unreliable_dc = dc;

            setup_dc(*dc);
            fire(::on_player_joined, id);
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
        info("You are {}", pid);
    }

    return pid;
}

bool Peer::is_offerer() const {
    return get_pid() > id;
}

static std::string get_blaster() {
    if (blaster == std::nullopt) {
        info("Using the default NutBlaster server as none was explicitly specified: {}", NUTBLAST_DEFAULT_SERVER);
        blaster = NUTBLAST_DEFAULT_SERVER;
    }

    return *blaster;
}

static int max_players = NUTBLAST_MAX_PLAYERS;

extern "C" void NutBlast_SetNutBlaster(const char* blaster) {
    ::blaster = blaster;
}

extern "C" void NutBlast_SetGameID(const char* gid) {
    ::gid = gid;
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

extern "C" const char* NutBlast_GetPeerField(NutBlast_ID pee, const char* name) {
    if (!pee || !name)
        return nullptr;

    if (pee == get_pid())
        return peer_meta.at(name).c_str();

    if (!NutBlast_IsReady())
        return nullptr;

    if (!::peers.contains(pee))
        return nullptr;

    const auto& peer = ::peers.at(pee);

    if (peer.meta.contains(name))
        return peer.meta.at(name).c_str();

    return nullptr;
}

extern "C" void NutBlast_SetPeerField(const char* key, const char* value) {
    if (!key || !value)
        return;

    peer_meta.insert_or_assign(key, value);

    ::ws_send({
        {"type", "SetPeerMeta"},
        {"key", key},
        {"value", value},
    });
}

extern "C" const char* NutBlast_GetLobbyField(const char* name) {
    if (!name || !NutBlast_IsReady())
        return nullptr;

    if (lobby_meta.contains(name))
        return lobby_meta.at(name).c_str();

    return nullptr;
}

extern "C" void NutBlast_SetLobbyField(const char* key, const char* value) {
    if (!key || !value)
        return;

    if (NutBlast_IsReady()) {
        NutBlast_ID master = NutBlast_GetMasterID();
        if (!master || master != NutBlast_GetOurID())
            return;
    }

    lobby_meta.insert_or_assign(key, value);

    ::ws_send({
        {"type", "SetLobbyMeta"},
        {"key", key},
        {"value", value},
    });
}

static void recv_shit();

static void join_pro() {
    rtc::Preload();

    get_blaster();
    ::master = 0, ::disconnection_reason = std::nullopt;
    ::ws_in.clear(), ::ws_out.clear();
    ::incoming_candidates.clear(), ::incoming_offers.clear();

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
        if (listing) {
            ::ws_send({
                {"type", "List"},
                {"gid", ::gid},
            });
        } else {
            ::ws_send({
                {"type", "Connect"},
                {"mode", ::hosting ? "Host" : "Join"},
                {"gid", ::gid},
                {"pid", ::get_pid()},
                {"lid", ::lid},
                {"lname", ::lname},
                {"max_players", ::max_players},
                {"peer_meta", ::peer_meta},
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

        recv_shit();

        info("NutBlaster out!");
        fire(::on_disconnected, ::disconnection_reason ? ::disconnection_reason->c_str() : nullptr);
    }

    if (::blaster_ws) { // `recv_shit` could've nuked the socket so we check for null once again
        try {
            ::blaster_ws->close();
        } catch (const std::runtime_error&) {}
    }

    ::blaster_ws = nullptr, ::lid = 0, ::lname = std::nullopt;
    ::peers.clear(), ::ws_in.clear(), ::ws_out.clear();
}

extern "C" void NutBlast_FindLobbies() {
    if (::blaster_ws) {
        info("You're already connected!");
    } else {
        ::listing = true;
        join_pro();
        info("Connecting to {}", get_blaster());
    }
}

extern "C" void NutBlast_Join(NutBlast_ID id) {
    if (::blaster_ws) {
        info("You're already connected!");
    } else if (!id) {
        info("No ID specified!");
    } else {
        ::hosting = false, ::listing = false;
        ::lid = id, ::lname = std::nullopt;

        join_pro();
        info("Trying to join '{}' at: {}", id, get_blaster());
    }
}

extern "C" void NutBlast_Host(NutBlast_ID id, const char* name, int max) {
    if (::blaster_ws) {
        info("You're already connected!");
    } else if (!name || !name[0]) {
        info("Lobby name cannot be null or empty");
    } else {
        NutBlast_SetMaxPlayers(max);
        ::hosting = true, ::listing = false;
        ::lid = id ? id : generate_id(), ::lname = name;

        join_pro();
        info("Trying to host '{}' at: {}", lid, get_blaster());
    }
}

extern "C" int NutBlast_GetPlayerCount() {
    if (!NutBlast_IsReady())
        return 0;
    return static_cast<int>(1 + peers.size());
}

extern "C" const NutBlast_ID* NutBlast_GetPlayerIDs() {
    static NutBlast_ID buf[NUTBLAST_MAX_PLAYERS + 1] = {0};

    size_t i = 0;

    for (const auto& [id, player] : peers)
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

    for (const auto& [key, player] : peers)
        if (id == key)
            return true;

    return false;
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

static void handle_list(const nlohmann::json& obj) {
    std::vector<NutBlast_Lobby> lobbies;
    std::vector<std::string> tmp;

    for (const auto& lober : obj["list"]) {
        tmp.push_back(lober["name"]);

        lobbies.push_back({
            .id = lober["lid"],
            .name = tmp.back().c_str(),
            .players = lober["players"],
            .capacity = lober["max"],
        });
    }

    const auto data = const_cast<const NutBlast_Lobby*>(lobbies.data());
    fire(::on_lobbies_found, data, lobbies.size());
}

static const std::unordered_map<std::string, void (*)(const nlohmann::json&)> payload_types{
    {"Bye",
        [](const auto& obj) {
            ::disconnection_reason = obj["reason"];
            NutBlast_Disconnect();
        }},
    {"CapacitySet",
        [](const auto& obj) {
            ::max_players = obj["capacity"];
        }},
    {"PeerMetaSet",
        [](const auto& obj) {
            if (::peers.contains(obj["peer"]))
                ::peers.at(obj["peer"]).meta.insert_or_assign(obj["key"], obj["value"]);
        }},
    {"LobbyMetaSet",
        [](const auto& obj) {
            ::lobby_meta.insert_or_assign(obj["key"], obj["value"]);
        }},
    {"NewMaster",
        [](const auto& obj) {
            ::master = obj["id"];
        }},
    {"Joined",
        [](const auto& obj) {
            const NutBlast_ID id = obj["id"];
            ::peers.insert({id, Peer(id, obj["meta"])});
        }},
    {"Left",
        [](const auto& obj) {
            if (::peers.contains(obj["id"]))
                ::peers.erase(obj["id"]);
        }},
    {"Offer", handle_offer_or_answer},
    {"Answer", handle_offer_or_answer},
    {"Candidate", handle_candidate},
    {"List", handle_list},
};

static void recv_shit() {
    for (const auto& obj : copy_and_clear(::ws_in)) {
        if (!obj.contains("type"))
            continue;

        const std::string type = obj["type"];

        if (!payload_types.contains(type))
            continue;

        payload_types.at(type)(obj);
    }

    for (auto& [id, peer] : ::peers)
        peer.state->drain_incoming_offers_and_candidates();
}

extern "C" void NutBlast_Flush() {
    static Ticker beater(interval::beat);

    if (!NutBlast_IsReady() || listing || !beater)
        return;

    for (auto& [id, peer] : ::peers) {
        for (const auto& candidate : copy_and_clear(peer.state->outgoing_candidates)) {
            ::ws_send({
                {"type", "PassCandidate"},
                {"to", id},
                {"candidate", (rtc::string)candidate},
                {"mid", candidate.mid()},
            });
        }
    }
}

extern "C" void NutBlast_Update() {
    recv_shit();
    NutBlast_Flush();
}

static void greatest_technician_thats_ever_lived(
    NutBlast_ChannelID chan, NutBlast_ID id, const char* msg, int size, bool reliable) {
    if (!id || !msg || !NutBlast_PeerAlive(id))
        return;

    if (size < 0)
        size = (int)std::strlen(msg) + 1;

    rtc::binary buf(1 + size);
    buf[0] = static_cast<std::byte>(chan);

    for (size_t i = 0; i < size; i++)
        buf[i + 1] = static_cast<std::byte>(msg[i]);

    try {
        const auto& peer = ::peers.at(id);
        const auto& dc = reliable ? peer.state->reliable_dc : peer.state->unreliable_dc;
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

extern "C" bool NutBlast_PeerAlive(NutBlast_ID id) {
    if (!id || !NutBlast_IsReady())
        return false;

    if (get_pid() == id)
        return true;

    if (!::peers.contains(id))
        return false;

    return peers.at(id).is_alive();
}

extern "C" bool NutBlast_IsReady() {
    return ::blaster_ws && ::blaster_ws->isOpen();
}

extern "C" void NutBlast_OnConnected(void (*cb)()) {
    ::on_connected = cb;
}

extern "C" void NutBlast_OnDisconnected(void (*cb)(const char*)) {
    ::on_disconnected = cb;
}

extern "C" void NutBlast_OnPlayerJoined(void (*cb)(NutBlast_ID)) {
    ::on_player_joined = cb;
}

extern "C" void NutBlast_OnPlayerLeft(void (*cb)(NutBlast_ID)) {
    ::on_player_left = cb;
}

extern "C" void NutBlast_OnLobbiesFound(void (*cb)(const NutBlast_Lobby*, size_t)) {
    ::on_lobbies_found = cb;
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
