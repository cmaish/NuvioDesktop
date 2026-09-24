package com.nuvio.app.features.player.desktop.cast

import co.touchlab.kermit.Logger
import com.nuvio.app.core.storage.DesktopStorage
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.net.InetAddress
import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import kotlin.concurrent.Volatile

/** What is playing locally, as needed to hand it to a Cast receiver. */
internal data class CastMediaSource(
    val url: String,
    val headers: Map<String, String>,
    val title: String,
    val subtitle: String,
    val imageUrl: String?,
    /** Label of the audio track playing locally (mpv includes the codec, e.g. "English · 5.1 · DTS"). */
    val audioTrackLabel: String? = null,
    /** Position of that track among the stream's audio tracks. */
    val audioTrackIndex: Int = 0,
    /** Duration known to the local player; a converted stream can't tell the receiver its length. */
    val durationMs: Long = 0L,
)

/** The subtitle the local player shows, mirrored onto the receiver. */
internal data class CastSubtitleSelection(
    /** Side-loaded subtitle (addon, stream-provided or downloaded file). */
    val externalUrl: String? = null,
    val externalName: String? = null,
    val externalLanguage: String? = null,
    /** Built-in track picked in the local player: its position among the player's subtitle tracks. */
    val embeddedIndex: Int? = null,
    val embeddedLanguage: String? = null,
    val embeddedName: String? = null,
    val delayMs: Int = 0,
    val style: CastTextTrackStyle? = null,
) {
    val isOff: Boolean get() = externalUrl == null && embeddedIndex == null && embeddedLanguage == null && embeddedName == null

    val isEmbedded: Boolean get() = externalUrl == null && !isOff

    /** A short name for the on-screen status, e.g. "English". */
    val displayName: String
        get() = when {
            externalUrl != null -> externalName?.takeIf(String::isNotBlank) ?: externalLanguage?.takeIf(String::isNotBlank) ?: "Subtitles"
            else -> embeddedName?.takeIf(String::isNotBlank) ?: embeddedLanguage?.takeIf(String::isNotBlank) ?: "Built-in subtitles"
        }
}

internal enum class CastConnectionState { Idle, Connecting, Connected }

/** Where casting is at, for the on-screen status. */
internal enum class CastPhase { Idle, Connecting, Preparing, Loading, Buffering, Playing, Paused, Finished, Reconnecting }

internal data class CastUiState(
    val isDiscovering: Boolean = false,
    val devices: List<CastDevice> = emptyList(),
    val connectionState: CastConnectionState = CastConnectionState.Idle,
    val activeDevice: CastDevice? = null,
    val status: CastMediaStatus? = null,
    val errorMessage: String? = null,
    val errorToken: Long = 0L,
    val phase: CastPhase = CastPhase.Idle,
    /** What is being prepared or retried, shown under the phase ("Getting subtitles ready"). */
    val phaseDetail: String? = null,
    /** How the video, audio and subtitles reach the TV, e.g. "DTS → stereo AAC". */
    val videoInfo: String = "",
    val audioInfo: String = "",
    val subtitleInfo: String = "",
    /** 0..1 while built-in subtitles are being read for burning in, otherwise null. */
    val subtitleProgress: Float? = null,
    /** Whether the receiver is (or is about to be) playing, as opposed to paused or stopped. */
    val wantsPlayback: Boolean = false,
)

/**
 * Process-wide Cast coordinator for the desktop player. Owns discovery, the single active
 * [CastSession] and the LAN [CastMediaServer]. All blocking network work runs on one worker
 * thread so commands reach the receiver in the order the user issued them.
 */
internal object DesktopCastManager {
    private val log = Logger.withTag("DesktopCast")
    private const val DISCOVERY_TIMEOUT_MS = 6_000L

    private val worker: ExecutorService = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "nuvio-cast-worker").apply { isDaemon = true }
    }
    private val timer: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "nuvio-cast-timer").apply { isDaemon = true }
    }
    private val listeners = mutableSetOf<() -> Unit>()
    private val mediaServer = CastMediaServer()
    private val transcoder = CastTranscoder { DesktopStorage.rootDir.resolve("cast-ffmpeg").toFile() }
    private val probes = ConcurrentHashMap<String, CastMediaProbe>()
    private val httpClient: OkHttpClient by lazy {
        OkHttpClient.Builder()
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build()
    }

    @Volatile
    var state: CastUiState = CastUiState()
        private set

    @Volatile private var session: CastSession? = null
    /** Bumped on every connect/stop so a connect still in flight can tell it was cancelled. */
    @Volatile private var generation = 0L
    @Volatile private var discoveryThread: Thread? = null
    @Volatile private var currentMedia: CastMediaSource? = null
    @Volatile private var currentSubtitles = CastSubtitleSelection()
    @Volatile private var loadedSubtitleKey: Triple<String?, Int, String?>? = null
    @Volatile private var hasSideLoadedTrack = false
    @Volatile private var pendingEmbeddedMatch = false
    @Volatile private var pendingAudioTrackIndex = 0
    @Volatile private var audioWarningKey: String? = null
    /** Bumped on every LOAD so delayed checks can tell the media they were scheduled for is gone. */
    @Volatile private var loadGeneration = 0L
    /** True while the receiver plays an ffmpeg conversion rather than the source itself. */
    @Volatile private var isConverting = false
    /** Source position where the converted stream starts; the receiver's clock starts at zero there. */
    @Volatile private var streamOffsetMs = 0L
    @Volatile private var convertedDurationMs = 0L
    /** Subtitle drawn into the converted video, or null when none is burned in. */
    @Volatile private var burnedSubtitleKey: BurnKey? = null
    @Volatile private var burnNoticeKey: String? = null
    @Volatile private var burnFile: File? = null
    @Volatile private var sideLoadActivationNudged = false
    @Volatile private var sessionEndedListener: ((positionMs: Long, wasPlaying: Boolean, reason: String?) -> Unit)? = null
    /** Built-in text subtitles being read for burning in; one at a time, for the current media. */
    @Volatile private var extraction: SubtitleExtraction? = null
    /** Last position the receiver reported while playing or paused, on the source's timeline. */
    @Volatile private var lastGoodPositionMs = 0L
    /** Whether the current load has played at all; a stream that never started is not retried. */
    @Volatile private var loadReachedPlayback = false
    @Volatile private var recoveryAttempts = 0
    @Volatile private var recoveredAtPositionMs = -1L
    /** The load being recovered from, so repeated "finished" reports for it start one reload. */
    @Volatile private var recoveryLoad = -1L
    /** When the last play/pause/load was sent; receiver reports right after it may still be stale. */
    @Volatile private var lastCommandAtMs = 0L
    /** Pause pressed before the receiver was ready; applied once the media is loaded. */
    @Volatile private var pauseRequestedWhileConnecting = false

    private data class BurnKey(val source: String, val delayMs: Int, val style: CastTextTrackStyle?)

    private class SubtitleExtraction(
        val key: String,
        val fromMs: Long,
        val file: File,
        val durationMs: Long,
    ) {
        @Volatile var progressMs = 0L
        @Volatile var done = false
        @Volatile var failed = false
        @Volatile var cancelled = false
    }

    val isCasting: Boolean
        get() = state.connectionState != CastConnectionState.Idle

    fun addListener(listener: () -> Unit) {
        synchronized(listeners) { listeners += listener }
    }

    fun removeListener(listener: () -> Unit) {
        synchronized(listeners) { listeners -= listener }
    }

    fun startDiscovery() {
        if (discoveryThread?.isAlive == true) return
        update { it.copy(isDiscovering = true) }
        discoveryThread = Thread({
            try {
                CastDiscovery.discover(
                    timeoutMs = DISCOVERY_TIMEOUT_MS,
                    isCancelled = { Thread.currentThread().isInterrupted },
                ) { device ->
                    update { current ->
                        val others = current.devices.filterNot { it.id == device.id }
                        current.copy(devices = (others + device).sortedBy { it.name.lowercase() })
                    }
                }
            } catch (error: Throwable) {
                log.w(error) { "cast discovery failed" }
            } finally {
                update { it.copy(isDiscovering = false) }
            }
        }, "nuvio-cast-discovery").apply {
            isDaemon = true
            start()
        }
    }

    /**
     * Starts casting [media] to [device] from [positionMs]. [onEnded] runs (on a background
     * thread) when the session finishes for any reason, with the last known receiver position.
     */
    fun connect(
        device: CastDevice,
        media: CastMediaSource,
        subtitles: CastSubtitleSelection,
        positionMs: Long,
        autoplay: Boolean,
        onEnded: (positionMs: Long, wasPlaying: Boolean, reason: String?) -> Unit,
    ) {
        if (state.connectionState != CastConnectionState.Idle) {
            if (state.activeDevice?.id == device.id) return
            stopCasting()
        }
        val connectGeneration = ++generation
        pauseRequestedWhileConnecting = false
        sessionEndedListener = onEnded
        currentMedia = media
        currentSubtitles = subtitles
        lastGoodPositionMs = positionMs
        recoveryAttempts = 0
        update {
            it.copy(
                connectionState = CastConnectionState.Connecting,
                activeDevice = device,
                status = CastMediaStatus(positionMs = positionMs),
                errorMessage = null,
                phase = CastPhase.Connecting,
                phaseDetail = null,
                videoInfo = "",
                audioInfo = "",
                subtitleInfo = "",
                subtitleProgress = null,
                wantsPlayback = autoplay,
            )
        }
        worker.execute {
            if (generation != connectGeneration) return@execute
            val created = newSession(device)
            try {
                session = created
                created.start()
                if (generation != connectGeneration) {
                    created.stop()
                    return@execute
                }
                loadMedia(created, media, subtitles, positionMs, autoplay && !pauseRequestedWhileConnecting)
                if (generation != connectGeneration) {
                    created.stop()
                    return@execute
                }
                if (pauseRequestedWhileConnecting) {
                    created.pause()
                    update { it.copy(wantsPlayback = false) }
                }
                update { it.copy(connectionState = CastConnectionState.Connected) }
                // Subtitle changes made while connecting were not applied yet.
                val latest = currentSubtitles
                if (latest != subtitles) {
                    currentSubtitles = subtitles
                    updateSubtitles(latest)
                }
            } catch (error: Throwable) {
                log.w(error) { "casting to ${device.name} failed" }
                created.stop()
                onSessionEnded(ended = created, reason = error.message ?: "Could not cast to ${device.name}", expected = false)
            }
        }
    }

    private fun newSession(device: CastDevice): CastSession {
        lateinit var created: CastSession
        created = CastSession(
            device = device,
            onStatus = { status -> if (session === created) onSessionStatus(status) },
            onEnded = { reason, connectionLost ->
                if (connectionLost && state.connectionState == CastConnectionState.Connected) {
                    reconnect(created, reason)
                } else {
                    onSessionEnded(ended = created, reason = reason, expected = false)
                }
            },
        )
        return created
    }

    /**
     * The connection to the TV dropped (Wi-Fi hiccup, TV busy). The receiver usually keeps
     * playing, so rejoin its session rather than handing playback back to this computer.
     */
    private fun reconnect(lost: CastSession, reason: String?) {
        if (session !== lost) return
        val reconnectGeneration = generation
        val device = lost.device
        val lastStatus = sourceStatus(lost)
        update { it.copy(phase = CastPhase.Reconnecting, phaseDetail = "Lost the connection to ${device.name}") }
        worker.execute {
            for (attempt in 1..RECONNECT_ATTEMPTS) {
                if (generation != reconnectGeneration || session !== lost) return@execute
                runCatching { Thread.sleep(RECONNECT_DELAY_MS * attempt) }
                val created = newSession(device)
                val joined = runCatching {
                    created.start(joinExisting = true)
                    session = created
                    if (created.refreshMediaStatus() == null) {
                        // The receiver app restarted or dropped the media: load it again.
                        val media = currentMedia ?: error("Nothing to cast")
                        loadMedia(created, media, currentSubtitles, lastStatus.positionMs, state.wantsPlayback)
                    }
                }.onFailure { error ->
                    log.w(error) { "reconnecting to ${device.name} failed (attempt $attempt)" }
                    if (session === created) session = lost
                    created.disconnect()
                }.isSuccess
                if (joined) {
                    log.d { "reconnected to ${device.name}" }
                    update { it.copy(phase = phaseFor(created.status()), phaseDetail = null) }
                    return@execute
                }
            }
            onSessionEnded(ended = lost, reason = reason ?: "Lost the connection to ${device.name}", expected = false, lastStatus = lastStatus)
        }
    }

    /** Replaces what the receiver plays, e.g. after the user picked another source or episode. */
    fun changeMedia(media: CastMediaSource, subtitles: CastSubtitleSelection, positionMs: Long) {
        currentMedia = media
        currentSubtitles = subtitles
        cancelExtraction()
        val active = session ?: return
        worker.execute {
            lastGoodPositionMs = positionMs
            recoveryAttempts = 0
            runCatching { loadMedia(active, media, subtitles, positionMs, autoplay = true) }
                .onFailure { error -> reportError(error.message ?: "Could not cast this stream") }
        }
    }

    fun updateSubtitles(subtitles: CastSubtitleSelection) {
        val previous = currentSubtitles
        currentSubtitles = subtitles
        if (previous == subtitles) return
        val active = session ?: return
        val media = currentMedia ?: return
        worker.execute {
            if (state.connectionState != CastConnectionState.Connected) return@execute
            val canBurn = transcoder.isAvailable
            val wanted = if (canBurn) subtitles.burnKey() else null
            val reload = when {
                // Burned-in subtitles are part of the picture: any change (including turning
                // them off) means re-encoding from the current position.
                burnedSubtitleKey != null -> wanted != burnedSubtitleKey
                wanted != null -> true
                subtitles.externalUrl != null -> subtitles.sideLoadKey() != loadedSubtitleKey
                else -> false
            }
            when {
                reload -> {
                    // Side-loaded and burned-in subtitles can only change by (re)loading the media.
                    val status = sourceStatus(active)
                    runCatching {
                        loadMedia(active, media, subtitles, status.positionMs, autoplay = state.wantsPlayback)
                    }.onFailure { error -> reportError(error.message ?: "Could not update subtitles") }
                }
                subtitles.externalUrl != null -> active.setActiveTextTracks(listOf(SIDE_LOADED_TRACK_ID), subtitles.style)
                subtitles.isOff -> {
                    cancelExtraction()
                    active.setActiveTextTracks(emptyList(), subtitles.style)
                    update { it.copy(subtitleInfo = "Off", subtitleProgress = null) }
                }
                else -> {
                    pendingEmbeddedMatch = true
                    applyEmbeddedMatch(active, active.status())
                }
            }
        }
    }

    /**
     * Mirrors a local audio track change. Only possible when the receiver itself lists the
     * stream's audio tracks (MP4, HLS, DASH); progressive MKV plays its default track.
     */
    fun selectAudioTrack(index: Int, label: String?) {
        val media = currentMedia ?: return
        if (media.audioTrackIndex == index) return
        val updated = media.copy(audioTrackIndex = index, audioTrackLabel = label)
        currentMedia = updated
        withSession { active ->
            if (isConverting || (CastTranscoder.needsAudioConversion(label) && transcoder.isAvailable)) {
                // ffmpeg picks the track; restart the stream with the new one.
                val status = sourceStatus(active)
                runCatching {
                    loadMedia(active, updated, currentSubtitles, status.positionMs, state.wantsPlayback)
                }.onFailure { error -> reportError(error.message ?: "Could not switch the audio track") }
                return@withSession
            }
            val status = active.status()
            val track = status.receiverAudioTracks.getOrNull(index)
            val stillLoading = status.playerState == CastPlayerState.Loading || status.durationMs <= 0L
            when {
                track != null -> active.setActiveAudioTrack(track.id)
                status.receiverAudioTracks.isEmpty() && stillLoading -> pendingAudioTrackIndex = index
                else -> reportError("${active.device.name} can't switch audio tracks for this stream.")
            }
        }
    }

    fun play() {
        if (session == null) return
        pauseRequestedWhileConnecting = false
        noteCommand(wantsPlayback = true)
        withSession { it.play() }
    }

    fun pause() {
        if (session == null) return
        if (state.connectionState == CastConnectionState.Connecting) pauseRequestedWhileConnecting = true
        noteCommand(wantsPlayback = false)
        withSession { it.pause() }
    }

    private fun noteCommand(wantsPlayback: Boolean) {
        lastCommandAtMs = System.currentTimeMillis()
        if (state.wantsPlayback != wantsPlayback) update { it.copy(wantsPlayback = wantsPlayback) }
    }

    fun seekTo(positionMs: Long) = withSession { active -> seekOn(active, positionMs) }

    fun seekBy(offsetMs: Long) = withSession { active ->
        val status = sourceStatus(active)
        val target = (status.positionMs + offsetMs).coerceAtLeast(0L)
        seekOn(active, if (status.durationMs > 0L) target.coerceAtMost(status.durationMs) else target)
    }

    /** Runs on the worker. A converted stream can't seek, so it restarts ffmpeg from [positionMs]. */
    private fun seekOn(active: CastSession, positionMs: Long) {
        lastGoodPositionMs = positionMs
        if (!isConverting) {
            active.seekTo(positionMs)
            return
        }
        val media = currentMedia ?: return
        runCatching { loadMedia(active, media, currentSubtitles, positionMs, autoplay = state.wantsPlayback) }
            .onFailure { error -> reportError(error.message ?: "Could not seek on ${active.device.name}") }
    }

    fun setPlaybackRate(rate: Float) = withSession { it.setPlaybackRate(rate) }

    fun setVolume(level: Float) = withSession { it.setVolume(level) }

    fun currentStatus(): CastMediaStatus? = session?.let(::sourceStatus) ?: state.status

    /** The receiver's status on the source's timeline (a converted stream restarts at zero). */
    private fun sourceStatus(active: CastSession): CastMediaStatus = toSourceTimeline(active.status())

    private fun toSourceTimeline(status: CastMediaStatus): CastMediaStatus {
        if (!isConverting) return status
        val duration = convertedDurationMs.takeIf { it > 0L }
            ?: status.durationMs.takeIf { it > 0L }?.plus(streamOffsetMs)
            ?: 0L
        return status.copy(positionMs = status.positionMs + streamOffsetMs, durationMs = duration)
    }

    /** Ends casting and stops the receiver app. [onEnded] from [connect] still runs. */
    fun stopCasting() {
        generation++
        val active = session ?: run {
            if (state.connectionState != CastConnectionState.Idle) {
                onSessionEnded(ended = null, reason = null, expected = true)
            }
            return
        }
        val status = sourceStatus(active)
        active.stop()
        onSessionEnded(ended = active, reason = null, expected = true, lastStatus = status)
    }

    private fun withSession(block: (CastSession) -> Unit) {
        val active = session ?: return
        if (state.connectionState != CastConnectionState.Connected) return
        worker.execute { runCatching { block(active) }.onFailure { error -> log.w(error) { "cast command failed" } } }
    }

    private fun setPhase(phase: CastPhase, detail: String? = null) {
        update { it.copy(phase = phase, phaseDetail = detail) }
    }

    private fun loadMedia(
        active: CastSession,
        media: CastMediaSource,
        subtitles: CastSubtitleSelection,
        positionMs: Long,
        autoplay: Boolean,
    ) {
        try {
            loadMediaSteps(active, media, subtitles, positionMs, autoplay)
        } finally {
            // A failed step must not leave "Preparing" on screen.
            if (state.phase == CastPhase.Preparing || state.phase == CastPhase.Reconnecting) {
                update { it.copy(phase = phaseFor(active.status()), phaseDetail = null) }
            }
        }
    }

    private fun loadMediaSteps(
        active: CastSession,
        media: CastMediaSource,
        subtitles: CastSubtitleSelection,
        positionMs: Long,
        autoplay: Boolean,
    ) {
        val host = active.device.host
        // Whatever was converting for the previous media or position is no longer wanted.
        mediaServer.stopProcessStreams()
        lastCommandAtMs = System.currentTimeMillis()
        update { it.copy(wantsPlayback = autoplay) }
        val needsConversion = CastTranscoder.needsAudioConversion(media.audioTrackLabel)
        val canConvert = transcoder.isAvailable
        val input = localPathOrUrl(media.url)
        if (!subtitles.isEmbedded) cancelExtraction()
        setPhase(CastPhase.Preparing, "Checking the stream")
        val probe by lazy {
            probes.getOrPut(media.url) {
                transcoder.probe(input, media.headers) ?: CastMediaProbe(durationMs = 0L, videoCodec = null)
            }
        }
        // Built-in subtitles, when ffmpeg can draw them into the picture. Adaptive streams
        // (HLS/DASH) list their own subtitle tracks, which the receiver shows itself.
        val embedded = subtitles.takeIf { it.isEmbedded && canConvert && !isAdaptive(media.url) }?.let { selection ->
            setPhase(CastPhase.Preparing, "Getting subtitles ready")
            EmbeddedPlan.resolve(selection, probe.subtitleStreams)
        }
        var embeddedVtt: String? = null
        var embeddedPending: CastSubtitleStream? = null
        if (embedded is EmbeddedPlan.Text) {
            val ready = awaitExtraction(media, input, embedded.stream, positionMs, maxOf(media.durationMs, probe.durationMs))
            if (ready != null) {
                embeddedVtt = runCatching {
                    CastSubtitleConverter.toWebVtt(ready.readText(), ready.name, subtitles.delayMs - positionMs.toInt(), plainText = true)
                }.getOrNull()
                if (embeddedVtt == null) reportError("The built-in subtitles have no lines to show.")
            } else if (extraction?.failed != true) {
                embeddedPending = embedded.stream
            }
        }
        if (embedded is EmbeddedPlan.Unmatched) {
            reportError("Couldn't find that built-in subtitle track to show on ${active.device.name}. Try another subtitle.")
        }
        // A subtitle enabled in the player is drawn into the picture when ffmpeg is available:
        // the receiver then only has to play video. Re-encoded video starts exactly at
        // positionMs, so the cues are shifted by that much.
        val externalVtt = subtitles.externalUrl?.takeIf { canConvert }?.let { url ->
            setPhase(CastPhase.Preparing, "Getting subtitles ready")
            runCatching {
                loadSubtitleVtt(url, subtitles.delayMs - positionMs.toInt(), plainText = true) ?: error("No subtitle lines could be read")
            }
                .onFailure { error ->
                    log.w(error) { "could not prepare subtitle for burning in" }
                    reportError("Couldn't load the selected subtitles for casting.")
                }
                .getOrNull()
        }
        val burnVtt = externalVtt ?: embeddedVtt
        val overlayStream = (embedded as? EmbeddedPlan.Bitmap)?.stream
        val burn = burnVtt != null || overlayStream != null
        val convert = canConvert && (burn || needsConversion)
        val contentUrl: String
        val contentType: String
        val receiverStartMs: Long
        var encoder: String? = null
        if (convert) {
            setPhase(CastPhase.Preparing, if (burn) "Starting the video conversion" else "Starting the audio conversion")
            encoder = if (burn) transcoder.videoEncoder() else null
            val spec = CastTranscodeSpec(
                inputUrl = input,
                headers = media.headers,
                startMs = positionMs,
                audioTrackIndex = media.audioTrackIndex,
                videoCodec = probe.videoCodec,
                burnSubtitlesFile = burnVtt?.let(::writeBurnSubtitle),
                subtitleStyle = subtitles.style,
                overlaySubtitleStream = overlayStream?.index,
                videoEncoder = encoder ?: SOFTWARE_H264_ENCODER,
            )
            // The receiver's zero is where ffmpeg starts: exactly positionMs when re-encoding,
            // the keyframe before it when copying the video.
            val streamStartMs = when {
                positionMs <= 0L -> 0L
                burn -> positionMs
                else -> transcoder.keyframeStart(input, media.headers, positionMs) ?: positionMs
            }
            contentUrl = mediaServer.publishProcessStream(host, "stream.mp4") { transcoder.start(spec) }
            contentType = "video/mp4"
            receiverStartMs = 0L
            isConverting = true
            streamOffsetMs = streamStartMs
            convertedDurationMs = maxOf(media.durationMs, probe.durationMs)
        } else {
            contentUrl = resolveContentUrl(host, media)
            contentType = resolveContentType(media)
            receiverStartMs = positionMs
            isConverting = false
            streamOffsetMs = 0L
            convertedDurationMs = 0L
        }
        burnedSubtitleKey = if (burn) subtitles.burnKey() else null
        sideLoadActivationNudged = false
        // Cue times follow the stream the receiver plays, which starts at the offset when converting.
        val subtitleShiftMs = subtitles.delayMs - streamOffsetMs.toInt()
        // With ffmpeg available subtitles are always burned in; a failed burn would fail here too.
        val sideLoaded = subtitles.externalUrl?.takeIf { !canConvert }?.let { url ->
            setPhase(CastPhase.Preparing, "Getting subtitles ready")
            runCatching { loadSubtitleVtt(url, subtitleShiftMs) ?: error("No subtitle lines could be read") }
                .onFailure { error ->
                    log.w(error) { "could not prepare subtitle for cast" }
                    reportError("Couldn't load the selected subtitles for casting.")
                }
                .getOrNull()
                ?.let { vtt ->
                    CastTextTrack(
                        id = SIDE_LOADED_TRACK_ID,
                        url = mediaServer.publishSubtitle(host, vtt),
                        name = subtitles.externalName?.takeIf { it.isNotBlank() } ?: "Subtitles",
                        language = subtitles.externalLanguage,
                    )
                }
        }
        hasSideLoadedTrack = sideLoaded != null
        loadedSubtitleKey = if (sideLoaded != null) subtitles.sideLoadKey() else null
        // Without ffmpeg (or for HLS/DASH), built-in subtitles are matched against the receiver's own tracks.
        pendingEmbeddedMatch = subtitles.isEmbedded && embedded == null
        // When converting, ffmpeg already picked the audio track.
        pendingAudioTrackIndex = if (convert) 0 else media.audioTrackIndex
        val thisLoad = ++loadGeneration
        loadReachedPlayback = false
        update {
            it.copy(
                videoInfo = describeVideo(convert, burn, encoder),
                audioInfo = describeAudio(media.audioTrackLabel, convert, needsConversion),
                subtitleInfo = describeSubtitles(subtitles, burn, sideLoaded != null, embeddedPending != null, embedded),
                subtitleProgress = if (embeddedPending != null) extractionProgress() else null,
            )
        }
        setPhase(CastPhase.Loading)
        log.d {
            "cast load contentType=$contentType convert=$convert burn=$burn overlay=${overlayStream?.index} " +
                "offsetMs=$streamOffsetMs subtitles=${sideLoaded != null} audio=${media.audioTrackLabel}"
        }
        active.load(
            CastLoadRequest(
                contentUrl = contentUrl,
                contentType = contentType,
                title = media.title,
                subtitle = media.subtitle,
                imageUrl = media.imageUrl?.takeIf { it.startsWith("http", ignoreCase = true) },
                startPositionMs = receiverStartMs,
                autoplay = autoplay,
                textTracks = listOfNotNull(sideLoaded),
                activeTextTrackIds = if (sideLoaded != null) listOf(SIDE_LOADED_TRACK_ID) else emptyList(),
                textTrackStyle = subtitles.style,
            ),
        )
        update { it.copy(phase = phaseFor(active.status()), phaseDetail = null) }
        val noticeKey = "${media.url}|${subtitles.burnKey()?.source}"
        if (burn && noticeKey != burnNoticeKey) {
            burnNoticeKey = noticeKey
            audioWarningKey = "${media.url}|${media.audioTrackLabel}"
            reportError("Adding subtitles to the video for ${active.device.name}. This uses more CPU while casting.")
        }
        val warningKey = "${media.url}|${media.audioTrackLabel}"
        if (warningKey != audioWarningKey) {
            audioWarningKey = warningKey
            when {
                convert -> reportError("Converting ${codecIn(media.audioTrackLabel.orEmpty())} audio for ${active.device.name}.")
                needsConversion -> unsupportedAudioWarning(media.audioTrackLabel, active.device.name)
                    ?.let { reportError("$it (ffmpeg, needed to convert it, wasn't found.)") }
            }
        }
        if (sideLoaded != null) {
            // The receiver fetches an active text track right after loading. If it never asks,
            // it cannot reach this computer — almost always a firewall blocking the media server.
            timer.schedule({
                val stillShown = session === active && loadGeneration == thisLoad && currentSubtitles.externalUrl != null
                if (stillShown && !mediaServer.wasSubtitleFetched(sideLoaded.url)) {
                    log.w { "receiver never fetched the subtitle track from ${sideLoaded.url}" }
                    reportError(
                        "${active.device.name} couldn't download subtitles from this computer. " +
                            "Allow Nuvio through your firewall on private networks, then try again.",
                    )
                }
            }, SUBTITLE_FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        }
    }

    /** How a built-in subtitle track can be shown on the TV. */
    private sealed interface EmbeddedPlan {
        /** Picture subtitles: overlaid while re-encoding. */
        data class Bitmap(val stream: CastSubtitleStream) : EmbeddedPlan
        /** Text subtitles: read out of the file first, then burned in like an addon subtitle. */
        data class Text(val stream: CastSubtitleStream) : EmbeddedPlan
        data object Unmatched : EmbeddedPlan

        companion object {
            fun resolve(selection: CastSubtitleSelection, streams: List<CastSubtitleStream>): EmbeddedPlan {
                val stream = matchEmbeddedStream(selection, streams) ?: return Unmatched
                return if (stream.isBitmap) Bitmap(stream) else Text(stream)
            }
        }
    }

    /**
     * Returns the extracted subtitle file when it is ready, waiting briefly: reading from disk
     * usually finishes in seconds. Otherwise reading goes on in the background and the stream
     * is reloaded with the subtitles once they are ready.
     */
    private fun awaitExtraction(
        media: CastMediaSource,
        input: String,
        stream: CastSubtitleStream,
        positionMs: Long,
        durationMs: Long,
    ): File? {
        val key = "${media.url}#${stream.index}"
        val existing = extraction
        val job = if (
            existing != null && existing.key == key && !existing.cancelled && !existing.failed &&
            existing.fromMs <= positionMs + EXTRACTION_SLACK_MS
        ) {
            existing
        } else {
            cancelExtraction()
            startExtraction(key, media, input, stream, positionMs, durationMs)
        }
        val deadline = System.currentTimeMillis() + EXTRACTION_WAIT_MS
        while (!job.done && !job.failed && !job.cancelled && System.currentTimeMillis() < deadline) {
            Thread.sleep(200)
        }
        return job.file.takeIf { job.done }
    }

    private fun startExtraction(
        key: String,
        media: CastMediaSource,
        input: String,
        stream: CastSubtitleStream,
        positionMs: Long,
        durationMs: Long,
    ): SubtitleExtraction {
        val isLocal = !input.startsWith("http://", ignoreCase = true) && !input.startsWith("https://", ignoreCase = true)
        // A local file is read whole (so seeking back keeps its subtitles); a network stream
        // only from a little before where casting starts, since it has to be downloaded.
        val fromMs = if (isLocal) 0L else (positionMs - EXTRACTION_SLACK_MS).coerceAtLeast(0L)
        val dir = DesktopStorage.rootDir.resolve("cast-subtitles").toFile().apply { mkdirs() }
        val job = SubtitleExtraction(key, fromMs, File(dir, "embedded-${System.currentTimeMillis()}.srt"), durationMs)
        extraction = job
        job.progressMs = fromMs
        log.d { "reading built-in subtitle stream ${stream.index} (${stream.codec}) from ${fromMs}ms" }
        Thread({
            val ok = runCatching {
                transcoder.extractSubtitles(
                    inputUrl = input,
                    headers = media.headers,
                    streamIndex = stream.index,
                    fromMs = fromMs,
                    output = job.file,
                    onProgress = { reachedMs ->
                        job.progressMs = reachedMs
                        if (extraction === job) publishExtractionProgress()
                    },
                    isCancelled = { job.cancelled },
                )
            }.getOrDefault(false)
            if (job.cancelled) return@Thread
            if (ok) job.done = true else job.failed = true
            worker.execute { onExtractionFinished(job) }
        }, "nuvio-cast-subtitles").apply {
            isDaemon = true
            start()
        }
        return job
    }

    private fun onExtractionFinished(job: SubtitleExtraction) {
        if (extraction !== job || job.cancelled) return
        val active = session ?: return
        val media = currentMedia ?: return
        if (job.failed) {
            update { it.copy(subtitleInfo = "${currentSubtitles.displayName} · couldn't be read", subtitleProgress = null) }
            reportError("Couldn't read the built-in subtitles for casting. Try a subtitle from the Addons tab.")
            return
        }
        // Still wanted, and not shown yet: reload from where the TV is so they appear.
        val selection = currentSubtitles
        if (!selection.isEmbedded || burnedSubtitleKey == selection.burnKey()) return
        if (!job.key.startsWith("${media.url}#")) return
        val status = sourceStatus(active)
        log.d { "built-in subtitles ready; reloading at ${status.positionMs}ms" }
        runCatching { loadMedia(active, media, selection, status.positionMs, state.wantsPlayback) }
            .onFailure { error -> reportError(error.message ?: "Could not add the subtitles") }
    }

    private fun extractionProgress(): Float? {
        val job = extraction ?: return null
        if (job.done || job.failed) return null
        val span = job.durationMs - job.fromMs
        if (span <= 0L) return 0f
        // Whole percent steps: finer changes would only flood the controls with updates.
        return (((job.progressMs - job.fromMs).toFloat() / span).coerceIn(0f, 1f) * 100).toInt() / 100f
    }

    private fun publishExtractionProgress() {
        if ((extraction?.durationMs ?: 0L) <= 0L) return
        val progress = extractionProgress() ?: return
        // Only while the TV is waiting for these subtitles.
        if (state.subtitleProgress == null || progress == state.subtitleProgress) return
        update {
            it.copy(
                subtitleProgress = progress,
                subtitleInfo = "${currentSubtitles.displayName} · getting ready ${(progress * 100).toInt()}%",
            )
        }
    }

    private fun cancelExtraction() {
        val job = extraction ?: return
        extraction = null
        job.cancelled = true
        job.file.delete()
    }

    /** Writes the shifted subtitle for ffmpeg's subtitles filter, replacing the previous one. */
    private fun writeBurnSubtitle(vtt: String): File {
        val dir = DesktopStorage.rootDir.resolve("cast-subtitles").toFile().apply { mkdirs() }
        burnFile?.delete()
        return File(dir, "burn-${System.currentTimeMillis()}.vtt").apply {
            writeText(vtt)
            burnFile = this
        }
    }

    private fun localPathOrUrl(url: String): String =
        if (url.startsWith("file:", ignoreCase = true)) File(URI(url)).absolutePath else url

    private fun resolveContentType(media: CastMediaSource): String {
        val url = media.url
        CastMediaServer.contentTypeFromExtension(url)?.let { return it }
        val probed = runCatching {
            when {
                url.startsWith("http://", ignoreCase = true) || url.startsWith("https://", ignoreCase = true) ->
                    mediaServer.probeContentType(url, media.headers)
                else -> {
                    val file = if (url.startsWith("file:", ignoreCase = true)) File(URI(url)) else File(url)
                    file.inputStream().use { CastMediaServer.sniffContentType(it.readNBytes(16)) }
                }
            }
        }.onFailure { error -> log.d { "content type probe failed: ${error.message}" } }.getOrNull()
        return probed ?: "video/mp4"
    }

    private fun resolveContentUrl(receiverHost: String, media: CastMediaSource): String {
        val url = media.url
        if (url.startsWith("file:", ignoreCase = true)) {
            return mediaServer.publishFile(receiverHost, File(URI(url)))
        }
        if (!url.startsWith("http://", ignoreCase = true) && !url.startsWith("https://", ignoreCase = true)) {
            val file = File(url)
            if (file.isFile) return mediaServer.publishFile(receiverHost, file)
            return url
        }
        val needsProxy = media.headers.isNotEmpty() || isLocalOnlyHost(url)
        return if (needsProxy) mediaServer.publishProxy(receiverHost, url, media.headers) else url
    }

    private fun loadSubtitleVtt(url: String, delayMs: Int, plainText: Boolean = false): String? {
        val text = when {
            url.startsWith("http://", ignoreCase = true) || url.startsWith("https://", ignoreCase = true) -> {
                val request = Request.Builder().url(url).build()
                httpClient.newCall(request).execute().use { response ->
                    if (!response.isSuccessful) error("Subtitle request failed with HTTP ${response.code}")
                    response.body?.string().orEmpty()
                }
            }
            url.startsWith("file:", ignoreCase = true) -> File(URI(url)).readText()
            else -> File(url).readText()
        }
        return CastSubtitleConverter.toWebVtt(text, url, delayMs, plainText)
    }

    private fun onSessionStatus(status: CastMediaStatus) {
        val active = session
        val source = toSourceTimeline(status)
        if (active != null && recoverIfInterrupted(active, source)) return
        if (status.playerState == CastPlayerState.Playing || status.playerState == CastPlayerState.Paused) {
            lastGoodPositionMs = source.positionMs
            loadReachedPlayback = true
            // Played well past the last recovery: a later interruption gets fresh retries.
            if (recoveredAtPositionMs >= 0 && source.positionMs - recoveredAtPositionMs > RECOVERY_RESET_MS) {
                recoveryAttempts = 0
                recoveredAtPositionMs = -1L
            }
        }
        if (active != null && pendingEmbeddedMatch) {
            worker.execute { applyEmbeddedMatch(active, status) }
        }
        if (
            active != null && hasSideLoadedTrack && !sideLoadActivationNudged &&
            status.playerState == CastPlayerState.Playing && SIDE_LOADED_TRACK_ID !in status.activeTrackIds
        ) {
            // Some receivers ignore activeTrackIds in LOAD; switch the track on once playing.
            sideLoadActivationNudged = true
            worker.execute { active.setActiveTextTracks(listOf(SIDE_LOADED_TRACK_ID), currentSubtitles.style) }
        }
        if (active != null && pendingAudioTrackIndex > 0 && status.receiverAudioTracks.isNotEmpty()) {
            val index = pendingAudioTrackIndex
            pendingAudioTrackIndex = 0
            status.receiverAudioTracks.getOrNull(index)?.let { track ->
                worker.execute { active.setActiveAudioTrack(track.id) }
            }
        }
        // Play/pause pressed on the TV or its remote. Reports right after our own commands may
        // still describe the previous state, so they don't count.
        val settled = System.currentTimeMillis() - lastCommandAtMs > COMMAND_SETTLE_MS
        val wantsPlayback = when {
            !settled -> state.wantsPlayback
            status.playerState == CastPlayerState.Playing -> true
            status.playerState == CastPlayerState.Paused -> false
            status.isEnded -> false
            else -> state.wantsPlayback
        }
        update { current ->
            current.copy(
                status = source,
                // Explicit steps (connecting, preparing a stream) show until they finish.
                phase = if (current.phase in STEP_PHASES) current.phase else phaseFor(status),
                wantsPlayback = wantsPlayback,
            )
        }
        if (status.isFailed) reportError("${state.activeDevice?.name ?: "The Cast device"} could not play this stream")
    }

    /**
     * A stream that stops long before its end (the connection to the source dropped, ffmpeg
     * lost its input) looks "finished" to the receiver. Pick it up again from where it was
     * instead of treating the episode as over. Returns true when a reload was started.
     */
    private fun recoverIfInterrupted(active: CastSession, source: CastMediaStatus): Boolean {
        if (!source.isEnded && !source.isFailed) return false
        // The receiver keeps reporting the stopped media until the reload replaces it.
        if (recoveryLoad == loadGeneration) return true
        if (!shouldRecover(source.durationMs.takeIf { it > 0L } ?: convertedDurationMs, lastGoodPositionMs, loadReachedPlayback, recoveryAttempts)) {
            return false
        }
        val media = currentMedia ?: return false
        recoveryAttempts++
        recoveryLoad = loadGeneration
        recoveredAtPositionMs = lastGoodPositionMs
        val resumeAt = lastGoodPositionMs
        log.w { "cast stream stopped early at ${resumeAt}ms (${source.idleReason}); resuming (attempt $recoveryAttempts)" }
        update { it.copy(phase = CastPhase.Reconnecting, phaseDetail = "The stream was interrupted. Picking it up again") }
        worker.execute {
            if (session !== active) return@execute
            runCatching { loadMedia(active, media, currentSubtitles, resumeAt, autoplay = true) }
                .onFailure { error -> reportError(error.message ?: "Could not resume the stream") }
        }
        return true
    }

    private fun applyEmbeddedMatch(active: CastSession, status: CastMediaStatus) {
        if (!pendingEmbeddedMatch || hasSideLoadedTrack) return
        val tracks = status.receiverTextTracks
        if (tracks.isEmpty()) {
            val mediaStarted = status.durationMs > 0L &&
                (status.playerState == CastPlayerState.Playing || status.playerState == CastPlayerState.Paused)
            if (mediaStarted) {
                // The receiver found no text tracks of its own (e.g. subtitles inside an MKV).
                pendingEmbeddedMatch = false
                update { it.copy(subtitleInfo = "${currentSubtitles.displayName} · not available on the TV") }
                reportError(
                    "Built-in subtitles in this file can't be shown on ${active.device.name} without ffmpeg. " +
                        "Pick a subtitle from the Addons tab instead.",
                )
            }
            return
        }
        pendingEmbeddedMatch = false
        val selection = currentSubtitles
        val language = selection.embeddedLanguage?.lowercase()?.substringBefore('-')
        val match = tracks.firstOrNull { track ->
            language != null && track.language?.lowercase()?.substringBefore('-') == language
        } ?: tracks.firstOrNull { track ->
            selection.embeddedName != null && track.name.equals(selection.embeddedName, ignoreCase = true)
        }
        if (match != null) {
            active.setActiveTextTracks(listOf(match.id), selection.style)
            update { it.copy(subtitleInfo = "${selection.displayName} · TV subtitle track") }
        }
    }

    private fun onSessionEnded(ended: CastSession?, reason: String?, expected: Boolean, lastStatus: CastMediaStatus? = null) {
        synchronized(this) {
            // A session that was already replaced or stopped reports nothing further.
            if (ended != null && session !== ended) return
            if (ended == null && state.connectionState == CastConnectionState.Idle) return
        }
        val status = lastStatus ?: ended?.let(::sourceStatus) ?: state.status
        val wantedPlayback = state.wantsPlayback
        session = null
        val listener = sessionEndedListener
        sessionEndedListener = null
        currentMedia = null
        loadedSubtitleKey = null
        hasSideLoadedTrack = false
        pendingEmbeddedMatch = false
        pendingAudioTrackIndex = 0
        audioWarningKey = null
        isConverting = false
        streamOffsetMs = 0L
        convertedDurationMs = 0L
        burnedSubtitleKey = null
        burnNoticeKey = null
        recoveryAttempts = 0
        recoveredAtPositionMs = -1L
        cancelExtraction()
        worker.execute {
            mediaServer.stop()
            burnFile?.delete()
            burnFile = null
        }
        update { current ->
            current.copy(
                connectionState = CastConnectionState.Idle,
                activeDevice = null,
                status = null,
                errorMessage = if (expected) current.errorMessage else reason,
                errorToken = if (!expected && reason != null) current.errorToken + 1 else current.errorToken,
                phase = CastPhase.Idle,
                phaseDetail = null,
                videoInfo = "",
                audioInfo = "",
                subtitleInfo = "",
                subtitleProgress = null,
                wantsPlayback = false,
            )
        }
        val wasPlaying = when (status?.playerState) {
            CastPlayerState.Playing, CastPlayerState.Buffering -> true
            CastPlayerState.Paused -> false
            else -> wantedPlayback && status?.isEnded != true
        }
        listener?.invoke(status?.positionMs ?: 0L, wasPlaying, if (expected) null else reason)
    }

    private fun reportError(message: String) {
        update { it.copy(errorMessage = message, errorToken = it.errorToken + 1) }
    }

    private fun update(transform: (CastUiState) -> CastUiState) {
        val changed = synchronized(this) {
            val next = transform(state)
            val different = next != state
            state = next
            different
        }
        if (!changed) return
        val snapshot = synchronized(listeners) { listeners.toList() }
        snapshot.forEach { listener -> runCatching { listener() } }
    }

    private fun CastSubtitleSelection.sideLoadKey(): Triple<String?, Int, String?> =
        Triple(externalUrl, delayMs, externalLanguage)

    private fun CastSubtitleSelection.burnKey(): BurnKey? = when {
        externalUrl != null -> BurnKey(externalUrl, delayMs, style)
        embeddedIndex != null -> BurnKey("embedded:$embeddedIndex", delayMs, style)
        else -> null
    }

    private fun isAdaptive(url: String): Boolean {
        val type = CastMediaServer.contentTypeFromExtension(url) ?: return false
        return type.contains("mpegurl", ignoreCase = true) || type.contains("dash", ignoreCase = true)
    }

    private fun describeVideo(convert: Boolean, burn: Boolean, encoder: String?): String = when {
        burn -> "Converted to H.264 with subtitles" + if (encoder == SOFTWARE_H264_ENCODER) " (CPU)" else " (GPU)"
        convert -> "Original video"
        else -> "Streamed directly"
    }

    private fun describeAudio(label: String?, convert: Boolean, needsConversion: Boolean): String {
        val codec = label?.let { Regex("""DTS-HD|DTS|TrueHD|E-AC-3(-JOC)?|AC-3|AAC|Opus|FLAC|MP3|Vorbis""", RegexOption.IGNORE_CASE).find(it)?.value }
        return when {
            convert && needsConversion -> "${codec ?: "Audio"} → stereo AAC"
            convert -> "Stereo AAC"
            needsConversion -> "${codec ?: "Original"} (may not play on the TV)"
            codec != null -> codec
            else -> "Original audio"
        }
    }

    private fun describeSubtitles(
        subtitles: CastSubtitleSelection,
        burn: Boolean,
        sideLoaded: Boolean,
        pending: Boolean,
        embedded: EmbeddedPlan?,
    ): String {
        val name = subtitles.displayName
        return when {
            subtitles.isOff -> "Off"
            burn -> "$name · drawn into the video"
            pending -> "$name · getting ready"
            sideLoaded -> "$name · TV subtitle track"
            embedded is EmbeddedPlan.Unmatched -> "$name · not found"
            subtitles.isEmbedded && extraction?.failed == true -> "$name · couldn't be read"
            subtitles.isEmbedded -> "$name · looking for it on the TV"
            else -> "$name · couldn't be loaded"
        }
    }

    internal fun phaseFor(status: CastMediaStatus): CastPhase = when {
        status.isEnded -> CastPhase.Finished
        status.playerState == CastPlayerState.Playing -> CastPhase.Playing
        status.playerState == CastPlayerState.Paused -> CastPhase.Paused
        status.playerState == CastPlayerState.Buffering -> CastPhase.Buffering
        else -> CastPhase.Loading
    }

    /**
     * Whether an early stop should be resumed: the stream had played, it stopped more than
     * [RECOVERY_MIN_REMAINING_MS] before its end, and it hasn't been retried too often already.
     */
    internal fun shouldRecover(durationMs: Long, lastPositionMs: Long, reachedPlayback: Boolean, attempts: Int): Boolean =
        reachedPlayback && durationMs > 0L && attempts < RECOVERY_MAX_ATTEMPTS &&
            durationMs - lastPositionMs > RECOVERY_MIN_REMAINING_MS

    /**
     * Finds the ffmpeg subtitle stream for the track picked in the player. mpv numbers built-in
     * tracks in file order before any added ones, so the index usually matches; language and
     * name confirm it, and settle it when it doesn't.
     */
    internal fun matchEmbeddedStream(selection: CastSubtitleSelection, streams: List<CastSubtitleStream>): CastSubtitleStream? {
        if (streams.isEmpty()) return null
        val language = selection.embeddedLanguage?.lowercase()?.substringBefore('-')?.takeIf(String::isNotBlank)
        fun sameLanguage(stream: CastSubtitleStream): Boolean =
            language == null || stream.language == null || languageMatches(stream.language, language)
        selection.embeddedIndex?.let(streams::getOrNull)?.takeIf(::sameLanguage)?.let { return it }
        val candidates = streams.filter { stream -> language != null && stream.language != null && languageMatches(stream.language, language) }
        val name = selection.embeddedName?.lowercase().orEmpty()
        return candidates.firstOrNull { stream -> stream.title != null && name.contains(stream.title.lowercase()) }
            ?: candidates.firstOrNull()
    }

    /** Language tags may be two- or three-letter ("en", "eng"); both sides may use either form. */
    private fun languageMatches(a: String, b: String): Boolean {
        val left = a.lowercase().substringBefore('-')
        val right = b.lowercase().substringBefore('-')
        return left == right || iso3(left) == iso3(right)
    }

    private fun iso3(code: String): String =
        runCatching { java.util.Locale.forLanguageTag(code).isO3Language }.getOrNull()?.takeIf(String::isNotBlank) ?: code

    internal fun isLocalOnlyHost(url: String): Boolean {
        val host = runCatching { URI(url).host }.getOrNull()?.trim('[', ']') ?: return false
        if (host.equals("localhost", ignoreCase = true)) return true
        return runCatching {
            val address = InetAddress.getByName(host)
            address.isLoopbackAddress || address.isAnyLocalAddress
        }.getOrDefault(false)
    }

    /**
     * Receivers decode AAC, MP3, Opus, Vorbis and FLAC themselves. AC-3/E-AC-3 only play when
     * the TV or receiver passes them through, and DTS/TrueHD never do, so those streams play
     * silently on most Chromecasts.
     */
    internal fun unsupportedAudioWarning(label: String?, deviceName: String): String? {
        val text = label ?: return null
        return when {
            Regex("""\b(DTS(-HD)?|TrueHD)\b""", RegexOption.IGNORE_CASE).containsMatchIn(text) ->
                "$deviceName can't play this stream's audio (${codecIn(text)}), so there may be no sound. " +
                    "Try another audio track or a source with AAC audio."
            Regex("""\bE?-?AC-?3\b""", RegexOption.IGNORE_CASE).containsMatchIn(text) ->
                "This stream's audio (${codecIn(text)}) only plays if your TV supports it. " +
                    "If there's no sound, try another audio track or a source with AAC audio."
            else -> null
        }
    }

    private fun codecIn(label: String): String =
        Regex("""DTS-HD|DTS|TrueHD|E-AC-3(-JOC)?|AC-3""", RegexOption.IGNORE_CASE).find(label)?.value ?: "unsupported codec"

    private const val SIDE_LOADED_TRACK_ID = 1
    private const val SUBTITLE_FETCH_TIMEOUT_SECONDS = 15L
    private const val RECONNECT_ATTEMPTS = 3
    private const val RECONNECT_DELAY_MS = 1_500L
    private const val COMMAND_SETTLE_MS = 2_000L
    private const val RECOVERY_MAX_ATTEMPTS = 3
    private const val RECOVERY_MIN_REMAINING_MS = 30_000L
    private const val RECOVERY_RESET_MS = 5 * 60_000L
    /** How long a load waits for built-in subtitles before starting without them. */
    private const val EXTRACTION_WAIT_MS = 8_000L
    private const val EXTRACTION_SLACK_MS = 5_000L
    private val STEP_PHASES = setOf(CastPhase.Connecting, CastPhase.Preparing, CastPhase.Reconnecting)
}
