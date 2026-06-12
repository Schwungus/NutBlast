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
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <rtc/rtc.hpp>
#include <rtc/websocket.hpp>

#include <nlohmann/json.hpp>

#include <NutBlast.h>

static constexpr const bool WINDOSE =
#ifdef _WIN32
    true;
#else
    false;
#endif

using Metadata = std::unordered_map<std::string, std::string>;

static void (*on_connected)() = nullptr, (*on_disconnected)(const char*) = nullptr, (*on_player_joined)(const char*),
            (*on_player_left)(const char*);

static std::mutex sync;

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

struct PeerSharedState {
    std::shared_ptr<rtc::PeerConnection> pc = nullptr;
    std::shared_ptr<rtc::DataChannel> dc = nullptr;
    std::vector<rtc::Candidate> outgoing_candidates, incoming_candidates;

    void drain_incoming_candidates() {
        for (const auto& candidate : incoming_candidates) {
            try {
                pc->addRemoteCandidate(candidate);
            } catch (const std::logic_error&) { return; }
        }

        incoming_candidates.clear();
    }
};

struct Peer {
    Metadata meta;
    std::shared_ptr<PeerSharedState> state;
    const std::string id;

    Peer(const std::string&);
    ~Peer() = default;

    bool is_offerer() const;
};

struct Message {
    std::string from;
    std::vector<std::byte> bytes;
};

static std::string gid = "", pid = "";
static std::optional<std::string> blaster, lid, disconnect_reason;

static NutBlast_ChannelID max_chan = 1;
static std::array<std::vector<Message>, 1 << 8 * sizeof(max_chan)> recv_queues;

static bool hosting = false;
static std::string master = "";

static std::unordered_map<std::string, Peer> peers;

static std::shared_ptr<rtc::WebSocket> blaster_ws = nullptr;
static std::vector<std::string> ws_in;
static std::vector<nlohmann::json> ws_out;

static Metadata peer_meta, lobby_meta;

namespace ns {
    constexpr const std::uint64_t second = 1000000000;
};

namespace interval {
    constexpr const std::uint64_t beat = ::ns::second / 60;
};

template <typename... T> static inline void info(const char* fmt, T... args) {
    static char buf[1024] = "";
    std::snprintf(buf, sizeof(buf), "%s\n", fmt);
    std::printf(buf, args...);
    std::fflush(stdout);
}

// making this one available externally for internal use.
extern "C" uint64_t NutBlast_TimeNS() {
    struct timespec ts = {0};
    timespec_get(&ts, TIME_UTC);
    return (std::uint64_t)ts.tv_sec * ns::second + (std::uint64_t)ts.tv_nsec;
}

static void ws_send(nlohmann::json&& obj) {
    ws_out.push_back(obj);

    if (!::blaster_ws)
        ws_out.clear();

    if (!::blaster_ws || !::blaster_ws->isOpen())
        return;

    std::lock_guard guard(::sync);

    try {
        for (const auto& obj : ws_out)
            ::blaster_ws->send(obj.dump());
    } catch (const std::runtime_error&) { NutBlast_Disconnect(); }

    ws_out.clear();
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

Peer::Peer(const std::string& id) : state(new PeerSharedState()), id(id) {
    state->pc = std::make_shared<rtc::PeerConnection>(::rtc_config);
    const std::weak_ptr<PeerSharedState> st = state;

    state->pc->onLocalDescription([id, st](const auto& local_desc) {
        if (st.expired())
            return;

        std::string type;

        if (local_desc.typeString() == "offer")
            type = "Offer";
        else if (local_desc.typeString() == "answer")
            type = "Answer";
        else
            return;

        ::ws_send({
            {"type", type},
            {"to", id},
            {"sdp", (rtc::string)local_desc},
        });
    });

    state->pc->onLocalCandidate([st](const auto& candidate) {
        if (!st.expired())
            st.lock()->outgoing_candidates.push_back(candidate);
    });

    const auto setup_dc = [id](rtc::DataChannel& dc) {
        dc.onOpen([id]() {
            std::lock_guard guard(::sync);
            fire(::on_player_joined, id.c_str());
        });

        dc.onClosed([id]() {
            std::lock_guard guard(::sync);
            fire(::on_player_left, id.c_str());
        });

        dc.onMessage([id](const auto& variant) {
            if (!std::holds_alternative<rtc::binary>(variant))
                return;

            std::lock_guard guard(::sync);

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

    state->pc->onDataChannel([id, st, setup_dc](const auto& dc) {
        if (st.expired())
            return;

        const auto state = st.lock();
        state->dc = dc;
        setup_dc(*dc);
        fire(::on_player_joined, id.c_str());
    });

    if (is_offerer()) {
        state->dc = state->pc->createDataChannel("NutBlast");
        setup_dc(*state->dc);
    }
}

static std::string get_pid() {
    if (pid.empty()) {
        std::mt19937 mt;
        mt.seed(NutBlast_TimeNS());

        std::uniform_int_distribution dist;

        for (size_t i = 0; i < sizeof(NutBlast_PlayerID); i++)
            pid.push_back(static_cast<char>('A' + dist(mt) % ('Z' - 'A' + 1)));
    }

    return pid;
}

bool Peer::is_offerer() const {
    return get_pid() > id;
}

static std::string get_blaster() {
    if (blaster == std::nullopt) {
        info("Using the default NutBlaster server as none was explicitly specified: %s", NUTBLAST_DEFAULT_SERVER);
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
}

static bool is_connected() {
    return ::lid.has_value() && ::blaster_ws != nullptr;
}

extern "C" const char* NutBlast_GetPeerField(const char* pee, const char* name) {
    if (!pee || !name)
        return nullptr;

    if (pee == get_pid())
        return peer_meta.at(name).c_str();

    if (!is_connected())
        return nullptr;

    if (!::peers.contains(pee))
        return nullptr;

    const auto& peer = ::peers.at(pee);

    if (peer.meta.contains(name))
        return peer.meta.at(name).c_str();

    return nullptr;
}

extern "C" void NutBlast_SetPeerField(const char* name, const char* value) {
    peer_meta.insert_or_assign(name, value);
}

extern "C" const char* NutBlast_GetLobbyField(const char* name) {
    if (!is_connected())
        return nullptr;

    if (lobby_meta.contains(name))
        return lobby_meta.at(name).c_str();

    return nullptr;
}

extern "C" void NutBlast_SetLobbyField(const char* name, const char* value) {
    const char* master = NutBlast_GetMasterID();

    if (master != nullptr && master == get_pid())
        lobby_meta.insert_or_assign(name, value);
}

static void join_pro(const char* id) {
    get_blaster();
    ::lid = id, ::master = "", ::disconnect_reason = std::nullopt;
    ::ws_in.clear(), ::ws_out.clear();

    for (auto& queue : recv_queues)
        queue.clear();

    std::optional<rtc::string> ca = std::nullopt;

    if (!WINDOSE)
        ca = "/etc/ssl/certs/ca-certificates.crt";

    ::blaster_ws = std::make_shared<rtc::WebSocket>(
#ifndef __EMSCRIPTEN__
        rtc::WebSocketConfiguration{
            .caCertificatePemFile = ca,
        }
#endif
    );

    ::blaster_ws->onOpen([]() {
        fire(::on_connected);
    });

    ::blaster_ws->onMessage([](const auto& _msg) {
        if (std::holds_alternative<rtc::string>(_msg)) {
            const auto msg = std::get<rtc::string>(_msg);
            ws_in.push_back(std::move(msg));
        }
    });

    ::blaster_ws->onClosed([]() {
        NutBlast_Disconnect();
    });

    ::blaster_ws->open(get_blaster());

    info("Trying to %s '%s' at: %s", ::hosting ? "host" : "join", id, get_blaster().c_str());
}

extern "C" void NutBlast_Disconnect() {
    static bool recursed = false;

    if (recursed)
        return;

    // :face_vomiting:
    struct Guard {
        Guard() {
            recursed = true;
        }

        ~Guard() {
            recursed = false;
        }
    } recurse_guard;

    info("NutBlaster out!");
    fire(::on_disconnected, ::disconnect_reason ? ::disconnect_reason->c_str() : "Disconnected");

    if (::blaster_ws) {
        try {
            ::blaster_ws->close();
        } catch (const std::runtime_error&) {}
    }

    ::blaster_ws = nullptr, ::lid = std::nullopt;
    ::peers.clear();
}

extern "C" void NutBlast_Join(const char* id) {
    if (is_connected())
        info("You're already in a lobby!");
    else
        join_pro(id);
}

extern "C" void NutBlast_Host(const char* id, int max) {
    if (is_connected()) {
        info("You're already in a lobby!");
    } else {
        NutBlast_SetMaxPlayers(max);
        ::hosting = true;
        join_pro(id);
    }
}

extern "C" int NutBlast_GetPlayerCount() {
    if (!is_connected())
        return 0;
    return static_cast<int>(1 + peers.size());
}

extern "C" const char** NutBlast_GetPlayerIDs() {
    static const char* buf[NUTBLAST_MAX_PLAYERS + 1] = {0};

    size_t i = 0;

    for (const auto& [key, player] : peers)
        buf[i++] = key.c_str();
    buf[i] = nullptr;

    return buf;
}

extern "C" const char* NutBlast_GetOurID() {
    get_pid();
    return ::pid.c_str();
}

extern "C" const char* NutBlast_GetMasterID() {
    return is_connected() ? ::master.c_str() : nullptr;
}

extern "C" bool NutBlast_IsPlayerAlive(const char* id) {
    if (!is_connected() || !id)
        return false;

    if (id == get_pid())
        return true;

    for (const auto& [key, player] : peers)
        if (id == key)
            return true;

    return false;
}

static void handle_update(const nlohmann::json& obj) {
    ::master = obj["master"];
    ::lobby_meta = obj["meta"];

    std::unordered_set<std::string> present_peers;

    for (const auto& [id, peer] : obj["peers"].items())
        if (id != get_pid())
            present_peers.insert(id);

    std::erase_if(::peers, [present_peers](const auto& pair) {
        const bool erase = !present_peers.contains(pair.first);

        if (erase)
            info("Bye, %s", pair.first.c_str());

        return erase;
    });

    for (const auto& id : present_peers)
        if (!::peers.contains(id))
            ::peers.insert({id, Peer(id)});

    for (auto& [id, peer] : ::peers) {
        const auto as_recv = obj["peers"][id];

        if (id != get_pid() && as_recv.contains("meta"))
            peer.meta = as_recv["meta"];
    }
}

static void handle_offer_answer(const nlohmann::json& obj) {
    const std::string& id = obj["from"];

    if (!::peers.contains(id))
        return;

    auto& peer = ::peers.at(id);
    peer.state->pc->setRemoteDescription({obj["sdp"], obj["type"] == "Offer" ? "offer" : "answer"});
    peer.state->drain_incoming_candidates();
}

static void handle_candidate(const nlohmann::json& obj) {
    const std::string& id = obj["from"];

    if (!::peers.contains(id))
        return;

    auto& peer = ::peers.at(id);
    peer.state->incoming_candidates.emplace_back(obj["candidate"], obj["mid"]);
    peer.state->drain_incoming_candidates();
}

static void handle_bye(const nlohmann::json& obj) {
    ::disconnect_reason = obj["reason"];
    NutBlast_Disconnect();
}

static void recv_shit() {
    static const std::unordered_map<std::string, std::function<void(const nlohmann::json&)>> types{
        {"Bye", handle_bye},
        {"Update", handle_update},
        {"Offer", handle_offer_answer},
        {"Answer", handle_offer_answer},
        {"Candidate", handle_candidate},
    };

    for (const auto& msg : ::ws_in) {
        try {
            const auto obj = nlohmann::json::parse(msg);

            if (!obj.contains("type"))
                continue;

            const std::string type = obj["type"];

            if (types.contains(type))
                types.at(type)(obj);
        } catch (const nlohmann::json::parse_error&) { continue; }
    }

    ::ws_in.clear();
}

static void send_shit() {
    for (auto& [id, peer] : ::peers) {
        for (const auto& candidate : peer.state->outgoing_candidates) {
            ::ws_send({
                {"type", "Candidate"},
                {"to", id},
                {"candidate", (rtc::string)candidate},
                {"mid", candidate.mid()},
            });
        }

        peer.state->outgoing_candidates.clear();
    }

    ::ws_send({
        {"type", "Update"},
        {"mode", ::hosting ? "Host" : "Join"},
        {"gid", ::gid},
        {"pid", ::get_pid()},
        {"lid", ::lid},
        {"peer_meta", ::peer_meta},
        {"lobby_meta", ::lobby_meta},
    });

    ::hosting = false; // otherwise we'll get booted with "lobby already exists!"
}

extern "C" void NutBlast_Flush() {
    if (!is_connected())
        return;

    // TODO: this is literal snake oil for now since `NutBlast_Send` sends immediately...
}

extern "C" void NutBlast_Update() {
    static Ticker beater(interval::beat);

    if (!is_connected())
        return;

    recv_shit();
    NutBlast_Flush();

    if (beater)
        send_shit();
}

extern "C" void NutBlast_SendTo(NutBlast_ChannelID chan, const char* id, const char* msg, int size) {
    if (!is_connected())
        return;

    if (size < 0)
        size = (int)std::strlen(msg) + 1;

    rtc::binary buf(1 + size);
    buf[0] = static_cast<std::byte>(chan);

    for (size_t i = 0; i < size; i++)
        buf[i + 1] = static_cast<std::byte>(msg[i]);

    try {
        const auto& peer = ::peers.at(id);
        const auto dc = peer.state->dc;
        dc && dc->send(buf);
    } catch (const std::out_of_range&) {
    } catch (const std::runtime_error&) {}
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
    out->from = msg.from.c_str();

    return true;
}

extern "C" void NutBlast_OnConnected(void (*cb)()) {
    ::on_connected = cb;
}

extern "C" void NutBlast_OnDisconnected(void (*cb)(const char*)) {
    ::on_disconnected = cb;
}

extern "C" void NutBlast_OnPlayerJoined(void (*cb)(const char*)) {
    ::on_player_joined = cb;
}

extern "C" void NutBlast_OnPlayerLeft(void (*cb)(const char*)) {
    ::on_player_left = cb;
}
