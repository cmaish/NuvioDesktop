package com.nuvio.app.features.player.desktop.cast

import co.touchlab.kermit.Logger
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.net.InetAddress
import java.net.URI
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
)

/** The subtitle the local player shows, mirrored onto the receiver. */
internal data class CastSubtitleSelection(
    /** Side-loaded subtitle (addon, stream-provided or downloaded file). */
    val externalUrl: String? = null,
    val externalName: String? = null,
    val externalLanguage: String? = null,
    /** Built-in track picked in the local player; matched against the receiver's own tracks. */
    val embeddedLanguage: String? = null,
    val embeddedName: String? = null,
    val delayMs: Int = 0,
    val style: CastTextTrackStyle? = null,
) {
    val isOff: Boolean get() = externalUrl == null && embeddedLanguage == null && embeddedName == null
}

internal enum class CastConnectionState { Idle, Connecting, Connected }

internal data class CastUiState(
    val isDiscovering: Boolean = false,
    val devices: List<CastDevice> = emptyList(),
    val connectionState: CastConnectionState = CastConnectionState.Idle,
    val activeDevice: CastDevice? = null,
    val status: CastMediaStatus? = null,
    val errorMessage: String? = null,
    val errorToken: Long = 0L,
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
    @Volatile private var sessionEndedListener: ((positionMs: Long, wasPlaying: Boolean, reason: String?) -> Unit)? = null

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
        sessionEndedListener = onEnded
        currentMedia = media
        currentSubtitles = subtitles
        update {
            it.copy(
                connectionState = CastConnectionState.Connecting,
                activeDevice = device,
                status = CastMediaStatus(positionMs = positionMs),
                errorMessage = null,
            )
        }
        worker.execute {
            if (generation != connectGeneration) return@execute
            lateinit var created: CastSession
            created = CastSession(
                device = device,
                onStatus = ::onSessionStatus,
                onEnded = { reason -> onSessionEnded(ended = created, reason = reason, expected = false) },
            )
            try {
                session = created
                created.start()
                loadMedia(created, media, subtitles, positionMs, autoplay)
                if (generation != connectGeneration) {
                    created.stop()
                    return@execute
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

    /** Replaces what the receiver plays, e.g. after the user picked another source or episode. */
    fun changeMedia(media: CastMediaSource, subtitles: CastSubtitleSelection, positionMs: Long) {
        currentMedia = media
        currentSubtitles = subtitles
        val active = session ?: return
        worker.execute {
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
            val key = subtitles.sideLoadKey()
            when {
                subtitles.externalUrl != null && key != loadedSubtitleKey -> {
                    // A side-loaded track can only be attached by (re)loading the media.
                    val status = active.status()
                    runCatching {
                        loadMedia(
                            active,
                            media,
                            subtitles,
                            status.positionMs,
                            autoplay = status.playerState != CastPlayerState.Paused,
                        )
                    }.onFailure { error -> reportError(error.message ?: "Could not update subtitles") }
                }
                subtitles.externalUrl != null -> active.setActiveTextTracks(listOf(SIDE_LOADED_TRACK_ID), subtitles.style)
                subtitles.isOff -> active.setActiveTextTracks(emptyList(), subtitles.style)
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
        currentMedia = media.copy(audioTrackIndex = index, audioTrackLabel = label)
        withSession { active ->
            val status = active.status()
            val track = status.receiverAudioTracks.getOrNull(index)
            val stillLoading = status.playerState == CastPlayerState.Loading || status.durationMs <= 0L
            when {
                track != null -> {
                    active.setActiveAudioTrack(track.id)
                    unsupportedAudioWarning(label, active.device.name)?.let(::reportError)
                }
                status.receiverAudioTracks.isEmpty() && stillLoading -> pendingAudioTrackIndex = index
                else -> reportError("${active.device.name} can't switch audio tracks for this stream.")
            }
        }
    }

    fun play() = withSession { it.play() }

    fun pause() = withSession { it.pause() }

    fun seekTo(positionMs: Long) = withSession { it.seekTo(positionMs) }

    fun seekBy(offsetMs: Long) = withSession { active ->
        val status = active.status()
        val target = (status.positionMs + offsetMs).coerceAtLeast(0L)
        active.seekTo(if (status.durationMs > 0L) target.coerceAtMost(status.durationMs) else target)
    }

    fun setPlaybackRate(rate: Float) = withSession { it.setPlaybackRate(rate) }

    fun setVolume(level: Float) = withSession { it.setVolume(level) }

    fun currentStatus(): CastMediaStatus? = session?.status() ?: state.status

    /** Ends casting and stops the receiver app. [onEnded] from [connect] still runs. */
    fun stopCasting() {
        generation++
        val active = session ?: run {
            if (state.connectionState != CastConnectionState.Idle) {
                onSessionEnded(ended = null, reason = null, expected = true)
            }
            return
        }
        val status = active.status()
        active.stop()
        onSessionEnded(ended = active, reason = null, expected = true, lastStatus = status)
    }

    private fun withSession(block: (CastSession) -> Unit) {
        val active = session ?: return
        if (state.connectionState != CastConnectionState.Connected) return
        worker.execute { runCatching { block(active) }.onFailure { error -> log.w(error) { "cast command failed" } } }
    }

    private fun loadMedia(
        active: CastSession,
        media: CastMediaSource,
        subtitles: CastSubtitleSelection,
        positionMs: Long,
        autoplay: Boolean,
    ) {
        val host = active.device.host
        val contentUrl = resolveContentUrl(host, media)
        val contentType = resolveContentType(media)
        val sideLoaded = subtitles.externalUrl?.let { url ->
            runCatching { loadSubtitleVtt(url, subtitles.delayMs) ?: error("No subtitle lines could be read") }
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
        pendingEmbeddedMatch = sideLoaded == null && subtitles.externalUrl == null && !subtitles.isOff
        pendingAudioTrackIndex = media.audioTrackIndex
        val thisLoad = ++loadGeneration
        log.d { "cast load contentType=$contentType subtitles=${sideLoaded != null} audio=${media.audioTrackLabel}" }
        active.load(
            CastLoadRequest(
                contentUrl = contentUrl,
                contentType = contentType,
                title = media.title,
                subtitle = media.subtitle,
                imageUrl = media.imageUrl?.takeIf { it.startsWith("http", ignoreCase = true) },
                startPositionMs = positionMs,
                autoplay = autoplay,
                textTracks = listOfNotNull(sideLoaded),
                activeTextTrackIds = if (sideLoaded != null) listOf(SIDE_LOADED_TRACK_ID) else emptyList(),
                textTrackStyle = subtitles.style,
            ),
        )
        val warningKey = "${media.url}|${media.audioTrackLabel}"
        if (warningKey != audioWarningKey) {
            audioWarningKey = warningKey
            unsupportedAudioWarning(media.audioTrackLabel, active.device.name)?.let(::reportError)
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

    private fun loadSubtitleVtt(url: String, delayMs: Int): String? {
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
        return CastSubtitleConverter.toWebVtt(text, url, delayMs)
    }

    private fun onSessionStatus(status: CastMediaStatus) {
        val active = session
        if (active != null && pendingEmbeddedMatch) {
            worker.execute { applyEmbeddedMatch(active, status) }
        }
        if (active != null && pendingAudioTrackIndex > 0 && status.receiverAudioTracks.isNotEmpty()) {
            val index = pendingAudioTrackIndex
            pendingAudioTrackIndex = 0
            status.receiverAudioTracks.getOrNull(index)?.let { track ->
                worker.execute { active.setActiveAudioTrack(track.id) }
            }
        }
        update { it.copy(status = status) }
        if (status.isFailed) reportError("${state.activeDevice?.name ?: "The Cast device"} could not play this stream")
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
                reportError(
                    "Built-in subtitles in this file can't be shown on ${active.device.name}. " +
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
        if (match != null) active.setActiveTextTracks(listOf(match.id), selection.style)
    }

    private fun onSessionEnded(ended: CastSession?, reason: String?, expected: Boolean, lastStatus: CastMediaStatus? = null) {
        synchronized(this) {
            // A session that was already replaced or stopped reports nothing further.
            if (ended != null && session !== ended) return
            if (ended == null && state.connectionState == CastConnectionState.Idle) return
        }
        val status = lastStatus ?: ended?.status() ?: state.status
        session = null
        val listener = sessionEndedListener
        sessionEndedListener = null
        currentMedia = null
        loadedSubtitleKey = null
        hasSideLoadedTrack = false
        pendingEmbeddedMatch = false
        pendingAudioTrackIndex = 0
        audioWarningKey = null
        worker.execute { mediaServer.stop() }
        update { current ->
            current.copy(
                connectionState = CastConnectionState.Idle,
                activeDevice = null,
                status = null,
                errorMessage = if (expected) current.errorMessage else reason,
                errorToken = if (!expected && reason != null) current.errorToken + 1 else current.errorToken,
            )
        }
        val wasPlaying = status?.playerState == CastPlayerState.Playing || status?.playerState == CastPlayerState.Buffering
        listener?.invoke(status?.positionMs ?: 0L, wasPlaying, if (expected) null else reason)
    }

    private fun reportError(message: String) {
        update { it.copy(errorMessage = message, errorToken = it.errorToken + 1) }
    }

    private fun update(transform: (CastUiState) -> CastUiState) {
        synchronized(this) { state = transform(state) }
        val snapshot = synchronized(listeners) { listeners.toList() }
        snapshot.forEach { listener -> runCatching { listener() } }
    }

    private fun CastSubtitleSelection.sideLoadKey(): Triple<String?, Int, String?> =
        Triple(externalUrl, delayMs, externalLanguage)

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
}
