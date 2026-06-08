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

#include <ctime>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <rtc/rtc.hpp>
#include <rtc/websocket.hpp>

#include <nlohmann/json.hpp>

#include <NutBlast.h>

using Metadata = std::unordered_map<std::string, std::string>;

static const rtc::Configuration rtc_config{
    .iceServers = {{"stun:stun.l.google.com:19302"}, {"stun:stun.l.google.com:5349"},},
};

struct PeerSharedState {
    std::shared_ptr<rtc::PeerConnection> pc = nullptr;
    std::shared_ptr<rtc::DataChannel> dc = nullptr;

    std::optional<rtc::Description> local_desc = std::nullopt;
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
    bool offering = true;

    Peer();
    ~Peer() = default;
};

static std::string gid = "", pid = "";
static std::optional<std::string> blaster, lid;

static bool hosting = false;
static std::string master = "";

static std::unordered_map<std::string, Peer> peers;

static std::shared_ptr<rtc::WebSocket> blaster_ws = nullptr;
static std::vector<std::string> ws_in;

static Metadata peer_meta, lobby_meta;

namespace ns {
    constexpr const std::uint64_t second = 1000000000;
};

template <typename... T> static inline void info(const char* fmt, T... args) {
    static char buf[1024] = "";
    std::snprintf(buf, sizeof(buf), "%s\n", fmt);
    std::printf(buf, args...);
}

// making this one available externally for internal use.
extern "C" uint64_t NutBlast_TimeNS() {
    struct timespec ts = {0};
    timespec_get(&ts, TIME_UTC);
    return (std::uint64_t)ts.tv_sec * ns::second + (std::uint64_t)ts.tv_nsec;
}

Peer::Peer() : state(new PeerSharedState()) {
    std::weak_ptr<PeerSharedState> st = state;

    state->pc = std::make_shared<rtc::PeerConnection>(::rtc_config);

    state->pc->onLocalDescription([st](const auto& local_desc) {
        if (!st.expired())
            st.lock()->local_desc = local_desc;
    });

    state->pc->onLocalCandidate([st](const auto& candidate) {
        if (!st.expired())
            st.lock()->outgoing_candidates.push_back(candidate);
    });

    const auto setup_dc = [st](const auto& dc) {
        dc->onOpen([dc]() {
            dc->send("hi!!!");
        });

        dc->onMessage([dc](const auto& variant) {
            if (std::holds_alternative<rtc::string>(variant)) {
                const auto& msg = std::get<rtc::string>(variant);
                info("GOT: %s", msg.c_str());
            } else {
                info("GOT JUNK");
            }
        });
    };

    state->pc->onDataChannel([st, setup_dc](const auto& dc) {
        if (st.expired())
            return;

        const auto state = st.lock();
        state->dc = dc;
        setup_dc(dc);
        dc->send("hallo!!!");
    });

    state->dc = state->pc->createDataChannel("NutBlast");
    setup_dc(state->dc);
}

static std::string get_pid() {
    std::mt19937 mt;
    mt.seed(NutBlast_TimeNS());

    std::uniform_int_distribution<> dist;

    if (pid.empty())
        for (size_t i = 0; i < sizeof(NutBlast_PlayerID); i++)
            pid.push_back(static_cast<char>('A' + dist(mt) % ('Z' - 'A' + 1)));

    return pid;
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

extern "C" void NutBlast_SetMaxPlayers(int max) {
    if (max > 1 && max <= NUTBLAST_MAX_PLAYERS)
        ::max_players = max;
}

static bool is_connected() {
    return lid.has_value() && blaster_ws != nullptr && blaster_ws->isOpen();
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

static void join_pro(const char* id, bool host) {
    if (is_connected()) {
        info("You're already in a lobby!");
        return;
    }

    get_pid(), get_blaster();
    ::lid = id, ::hosting = host;
    ::master = "";

    blaster_ws = std::make_shared<rtc::WebSocket>();

    blaster_ws->onMessage([](const auto& _msg) {
        if (!std::holds_alternative<std::string>(_msg))
            return;

        const auto msg = std::get<std::string>(_msg);
        ws_in.push_back(std::move(msg));
    });

    blaster_ws->onClosed([]() {
        info("NutBlaster out!");
        NutBlast_Disconnect();
    });

    blaster_ws->open(get_blaster());

    info("Trying to %s '%s' at: %s", host ? "host" : "join", id, get_blaster().c_str());
}

extern "C" void NutBlast_Disconnect() {
    ::lid = std::nullopt;
    ::peers.clear();
}

extern "C" void NutBlast_Join(const char* id) {
    join_pro(id, false);
}

extern "C" void NutBlast_Host(const char* id, int max) {
    NutBlast_SetMaxPlayers(max);
    join_pro(id, true);
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

extern "C" void NutBlast_Flush() {
    if (!is_connected())
        return;

    std::unordered_map<std::string, nlohmann::json> ice;

    for (const auto& [id, peer] : ::peers) {
        if (peer.state->local_desc.has_value()) {
            const auto t = peer.state->local_desc->type();

            const nlohmann::json ninja{
                {"type", t == rtc::Description::Type::Offer ? "Offer" : "Answer"},
                {"to", id},
                {"sdp", (rtc::string)*peer.state->local_desc},
            };

            ::blaster_ws->send(ninja.dump());
            peer.state->local_desc = std::nullopt;
        }

        for (const auto& candidate : peer.state->outgoing_candidates) {
            const nlohmann::json ninja{
                {"type", "Candidate"},
                {"to", id},
                {"candidate", (rtc::string)candidate},
                {"mid", candidate.mid()},
            };

            ::blaster_ws->send(ninja.dump());
        }

        peer.state->outgoing_candidates.clear();
    }

    {
        const nlohmann::json payload = {
            {"type", "Update"},
            {"gid", ::gid},
            {"pid", ::get_pid()},
            {"lid", ::lid},
            {"peer_meta", ::peer_meta},
            {"lobby_meta", ::lobby_meta},
        };

        ::blaster_ws->send(payload.dump());
    }
}

static void recv_shit() {
    for (const auto& msg : ::ws_in) {
        const auto obj = nlohmann::json::parse(msg);

        if (obj["type"] == "Update") {
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
                    ::peers.insert({id, Peer()});

            for (auto& [id, peer] : ::peers) {
                const auto as_recv = obj["peers"][id];

                if (id != get_pid() && as_recv.contains("meta"))
                    peer.meta = as_recv["meta"];
            }
        } else if (obj["type"] == "Offer" || obj["type"] == "Answer") {
            const std::string& id = obj["peer"];

            if (!::peers.contains(id))
                return;

            auto& peer = ::peers.at(id);
            const rtc::Description desc(obj["sdp"], obj["type"] == "Offer" ? "offer" : "answer");
            const bool polite = ::get_pid() < id, is_offer = desc.typeString() == "offer";

            if (is_offer && peer.offering) {
                if (!polite)
                    continue;
                peer.offering = false;
            }

            peer.state->pc->setRemoteDescription(desc);
            peer.state->drain_incoming_candidates();
        } else if (obj["type"] == "Candidate") {
            const std::string& id = obj["peer"];

            if (!::peers.contains(id))
                return;

            auto& peer = ::peers.at(id);
            peer.state->incoming_candidates.emplace_back(obj["candidate"], obj["mid"]);
            peer.state->drain_incoming_candidates();
        } else {
            // junk...
        }
    }

    ::ws_in.clear();
}

extern "C" void NutBlast_Update() {
    if (is_connected()) {
        recv_shit();
        NutBlast_Flush();
    }
}
