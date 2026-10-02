(function () {
    function updateSelectedPair(pc) {
        pc.getStats().then(function (stats) {
            var transport = null;
            stats.forEach(function (r) { if (r.type === 'transport') transport = r; });
            if (!transport || !transport.selectedCandidatePairId) return;

            var pair = null;
            stats.forEach(function (r) {
                if (r.id === transport.selectedCandidatePairId) pair = r;
            });
            if (!pair) return;

            var local = null, remote = null;
            stats.forEach(function (r) {
                if (r.id === pair.localCandidateId) local = r;
                if (r.id === pair.remoteCandidateId) remote = r;
            });
            if (!local || !remote) return;

            pc.__selectedCandidatePair = {
                localType: local.candidateType || '',
                remoteType: remote.candidateType || ''
            };
        });
    }

    var origRegister = WEBRTC.registerPeerConnection;
    WEBRTC.registerPeerConnection = function (pc) {
        var id = origRegister.call(WEBRTC, pc);

        var origIce = pc.oniceconnectionstatechange;
        pc.oniceconnectionstatechange = function () {
            if (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed')
                updateSelectedPair(pc);
            origIce();
        };

        var origSignaling = pc.onsignalingstatechange;
        pc.onsignalingstatechange = function () {
            if (pc.signalingState === 'stable' && (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed'))
                updateSelectedPair(pc);
            origSignaling();
        };

        return id;
    };
})();
