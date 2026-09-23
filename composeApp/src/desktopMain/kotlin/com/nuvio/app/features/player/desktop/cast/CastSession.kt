package com.nuvio.app.features.player.desktop.cast

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.Volatile

internal data class CastTextTrack(
    val id: Int,
    val url: String,
    val name: String,
    val language: String?,
)

internal data class CastTextTrackStyle(
    val foregroundColor: String,
    val edgeColor: String?,
    val fontScale: Float,
    val bold: Boolean,
)

internal data class CastLoadRequest(
    val contentUrl: String,
    val contentType: String,
    val title: String,
    val subtitle: String,
    val imageUrl: String?,
    val startPositionMs: Long,
    val autoplay: Boolean,
    val textTracks: List<CastTextTrack>,
    val activeTextTrackIds: List<Int>,
    val textTrackStyle: CastTextTrackStyle?,
)

/** A text track the receiver itself found inside the media (e.g. HLS WebVTT renditions). */
internal data class CastReceiverTrack(
    val id: Int,
    val name: String,
    val language: String?,
)

internal enum class CastPlayerState { Idle, Loading, Buffering, Playing, Paused }

internal data class CastMediaStatus(
    val playerState: CastPlayerState = CastPlayerState.Loading,
    val idleReason: String? = null,
    val positionMs: Long = 0L,
    val durationMs: Long = 0L,
    val playbackRate: Float = 1f,
    val volumeLevel: Float? = null,
    val muted: Boolean = false,
    val activeTrackIds: List<Int> = emptyList(),
    val receiverTextTracks: List<CastReceiverTrack> = emptyList(),
    val receiverAudioTracks: List<CastReceiverTrack> = emptyList(),
) {
    val isEnded: Boolean get() = playerState == CastPlayerState.Idle && idleReason == "FINISHED"
    val isFailed: Boolean get() = playerState == CastPlayerState.Idle && idleReason == "ERROR"
}

/**
 * Drives the Default Media Receiver on one Cast device: launch, load, transport controls and
 * status. Status updates are pushed by the receiver on every state change; between them the
 * playback position is extrapolated so the local timeline keeps moving smoothly.
 */
internal class CastSession(
    val device: CastDevice,
    private val onStatus: (CastMediaStatus) -> Unit,
    private val onEnded: (reason: String?) -> Unit,
) {
    private val log = Logger.withTag("CastSession")
    private val requestIds = AtomicInteger(1)
    private val pendingRequests = ConcurrentHashMap<Int, CompletableFuture<JsonObject>>()
    private val appReady = CompletableFuture<Pair<String, String>>()
    private val channel = CastChannel(
        host = device.host,
        port = device.port,
        onMessage = ::handleMessage,
        onClosed = { error -> finish(error?.message ?: "Connection closed") },
    )

    @Volatile private var transportId: String? = null
    @Volatile private var sessionId: String? = null
    @Volatile private var mediaSessionId: Int? = null
    @Volatile private var ended = false
    @Volatile private var lastStatus = CastMediaStatus()
    @Volatile private var lastStatusAtMs = System.currentTimeMillis()
    @Volatile private var receiverVolume: Float? = null
    @Volatile private var receiverMuted = false
    /** Text track ids we side-loaded with the current media; the receiver's own ones come from status. */
    @Volatile private var sideLoadedTextTrackIds: Set<Int> = emptySet()
    private var pollThread: Thread? = null

    /** Connects and launches the Default Media Receiver. Blocking; call off the UI thread. */
    fun start() {
        channel.connect()
        channel.send(CastNamespaces.CONNECTION, CastChannel.RECEIVER_ID, connectPayload())
        val launchId = nextRequestId()
        channel.send(
            CastNamespaces.RECEIVER,
            CastChannel.RECEIVER_ID,
            buildJsonObject {
                put("type", "LAUNCH")
                put("appId", DEFAULT_MEDIA_RECEIVER_APP_ID)
                put("requestId", launchId)
            }.toString(),
        )
        val (transport, session) = try {
            appReady.get(LAUNCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        } catch (error: Exception) {
            channel.close()
            throw IllegalStateException("${device.name} did not start the cast receiver", error)
        }
        transportId = transport
        sessionId = session
        channel.send(CastNamespaces.CONNECTION, transport, connectPayload())
        pollThread = Thread({ pollLoop() }, "nuvio-cast-status").apply {
            isDaemon = true
            start()
        }
    }

    /** Loads media and waits for the receiver to accept it. Blocking; call off the UI thread. */
    fun load(request: CastLoadRequest) {
        val transport = transportId ?: error("Cast session is not started")
        val requestId = nextRequestId()
        val future = CompletableFuture<JsonObject>()
        pendingRequests[requestId] = future
        sideLoadedTextTrackIds = request.textTracks.map { it.id }.toSet()
        lastStatus = lastStatus.copy(
            playerState = CastPlayerState.Loading,
            idleReason = null,
            positionMs = request.startPositionMs,
            activeTrackIds = request.activeTextTrackIds,
            receiverTextTracks = emptyList(),
            receiverAudioTracks = emptyList(),
        )
        lastStatusAtMs = System.currentTimeMillis()
        channel.send(CastNamespaces.MEDIA, transport, buildLoadPayload(request, requestId, sessionId).toString())
        val response = try {
            future.get(LOAD_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        } catch (error: Exception) {
            throw IllegalStateException("${device.name} did not respond to the load request", error)
        } finally {
            pendingRequests.remove(requestId)
        }
        when (val type = response["type"]?.jsonPrimitive?.contentOrNull) {
            "MEDIA_STATUS" -> Unit
            else -> {
                val reason = response["reason"]?.jsonPrimitive?.contentOrNull
                throw IllegalStateException(
                    "${device.name} could not play this stream" + (reason ?: type)?.let { " ($it)" }.orEmpty(),
                )
            }
        }
    }

    fun play() = sendMediaCommand("PLAY")

    fun pause() = sendMediaCommand("PAUSE")

    fun seekTo(positionMs: Long) {
        sendMediaCommand("SEEK") { put("currentTime", positionMs.coerceAtLeast(0L) / 1000.0) }
        lastStatus = lastStatus.copy(positionMs = positionMs.coerceAtLeast(0L))
        lastStatusAtMs = System.currentTimeMillis()
    }

    fun setPlaybackRate(rate: Float) = sendMediaCommand("SET_PLAYBACK_RATE") { put("playbackRate", rate.toDouble()) }

    /**
     * Activates [trackIds] as the text tracks. activeTrackIds covers every track type, so the
     * currently active audio/video tracks are carried over; sending only text ids would switch
     * the receiver's audio off.
     */
    fun setActiveTextTracks(trackIds: List<Int>, style: CastTextTrackStyle?) {
        val status = lastStatus
        val textIds = sideLoadedTextTrackIds + status.receiverTextTracks.map { it.id }
        editActiveTracks(mergeActiveTrackIds(status.activeTrackIds, textIds, trackIds), style)
    }

    /** Switches to one of the audio tracks the receiver reported for the current media. */
    fun setActiveAudioTrack(trackId: Int) {
        val status = lastStatus
        val audioIds = status.receiverAudioTracks.map { it.id }.toSet()
        if (trackId !in audioIds) return
        editActiveTracks(mergeActiveTrackIds(status.activeTrackIds, audioIds, listOf(trackId)), style = null)
    }

    private fun editActiveTracks(activeIds: List<Int>, style: CastTextTrackStyle?) {
        sendMediaCommand("EDIT_TRACKS_INFO") {
            putJsonArray("activeTrackIds") { activeIds.forEach { add(JsonPrimitive(it)) } }
            style?.let { put("textTrackStyle", it.toJson()) }
        }
        lastStatus = lastStatus.copy(activeTrackIds = activeIds)
    }

    fun setVolume(level: Float) {
        val transportReady = transportId != null
        if (!transportReady || ended) return
        runCatching {
            channel.send(
                CastNamespaces.RECEIVER,
                CastChannel.RECEIVER_ID,
                buildJsonObject {
                    put("type", "SET_VOLUME")
                    put("requestId", nextRequestId())
                    putJsonObject("volume") {
                        put("level", level.coerceIn(0f, 1f).toDouble())
                        put("muted", false)
                    }
                }.toString(),
            )
        }.onFailure { error -> log.w(error) { "set volume failed" } }
    }

    /** Current status with the position extrapolated from the last receiver report. */
    fun status(): CastMediaStatus {
        val status = lastStatus
        if (status.playerState != CastPlayerState.Playing) return status
        val elapsed = System.currentTimeMillis() - lastStatusAtMs
        val extrapolated = status.positionMs + (elapsed * status.playbackRate).toLong()
        val bounded = if (status.durationMs > 0L) extrapolated.coerceAtMost(status.durationMs) else extrapolated
        return status.copy(positionMs = bounded)
    }

    /** Stops playback on the receiver and closes the receiver app, then disconnects. */
    fun stop() {
        if (ended) return
        runCatching {
            sessionId?.let { session ->
                channel.send(
                    CastNamespaces.RECEIVER,
                    CastChannel.RECEIVER_ID,
                    buildJsonObject {
                        put("type", "STOP")
                        put("sessionId", session)
                        put("requestId", nextRequestId())
                    }.toString(),
                )
            }
        }
        disconnect()
    }

    /** Disconnects without touching what is playing on the receiver. */
    fun disconnect() {
        ended = true
        val stopped = IllegalStateException("Casting stopped")
        if (!appReady.isDone) appReady.completeExceptionally(stopped)
        pendingRequests.values.forEach { it.completeExceptionally(stopped) }
        pendingRequests.clear()
        runCatching {
            transportId?.let { channel.send(CastNamespaces.CONNECTION, it, """{"type":"CLOSE"}""") }
        }
        channel.close()
        pollThread?.interrupt()
        pollThread = null
    }

    private fun sendMediaCommand(type: String, extra: JsonObjectBuilder.() -> Unit = {}) {
        val transport = transportId ?: return
        val media = mediaSessionId ?: return
        if (ended) return
        runCatching {
            channel.send(
                CastNamespaces.MEDIA,
                transport,
                buildJsonObject {
                    put("type", type)
                    put("requestId", nextRequestId())
                    put("mediaSessionId", media)
                    extra()
                }.toString(),
            )
        }.onFailure { error -> log.w(error) { "media command $type failed" } }
    }

    private fun pollLoop() {
        try {
            while (!ended) {
                Thread.sleep(STATUS_POLL_INTERVAL_MS)
                val transport = transportId ?: continue
                runCatching {
                    channel.send(
                        CastNamespaces.MEDIA,
                        transport,
                        buildJsonObject {
                            put("type", "GET_STATUS")
                            put("requestId", nextRequestId())
                            mediaSessionId?.let { put("mediaSessionId", it) }
                        }.toString(),
                    )
                }
            }
        } catch (_: InterruptedException) {
            // Session ended.
        }
    }

    private fun handleMessage(namespace: String, sourceId: String, payload: JsonObject) {
        val type = payload["type"]?.jsonPrimitive?.contentOrNull
        val requestId = payload["requestId"]?.jsonPrimitive?.intOrNull
        when (namespace) {
            CastNamespaces.CONNECTION -> if (type == "CLOSE" && sourceId == transportId) {
                finish("The receiver closed the session")
            }
            CastNamespaces.RECEIVER -> when (type) {
                "RECEIVER_STATUS" -> handleReceiverStatus(payload)
                "LAUNCH_ERROR" -> appReady.completeExceptionally(
                    IllegalStateException(payload["reason"]?.jsonPrimitive?.contentOrNull ?: "Launch failed"),
                )
            }
            CastNamespaces.MEDIA -> {
                if (type == "MEDIA_STATUS") handleMediaStatus(payload)
                if (requestId != null && requestId != 0) {
                    pendingRequests.remove(requestId)?.complete(payload)
                }
            }
        }
    }

    private fun handleReceiverStatus(payload: JsonObject) {
        val status = payload["status"]?.asObject() ?: return
        status["volume"]?.asObject()?.let { volume ->
            receiverVolume = volume["level"]?.jsonPrimitive?.doubleOrNull?.toFloat() ?: receiverVolume
            receiverMuted = volume["muted"]?.jsonPrimitive?.booleanOrNull ?: receiverMuted
            publish(lastStatus.copy(volumeLevel = receiverVolume, muted = receiverMuted), resetClock = false)
        }
        val applications = status["applications"]?.asArray().orEmpty().mapNotNull { it.asObject() }
        val ours = applications.firstOrNull {
            it["appId"]?.jsonPrimitive?.contentOrNull == DEFAULT_MEDIA_RECEIVER_APP_ID
        }
        if (ours != null) {
            val transport = ours["transportId"]?.jsonPrimitive?.contentOrNull
            val session = ours["sessionId"]?.jsonPrimitive?.contentOrNull
            if (transport != null && session != null) {
                if (!appReady.isDone) appReady.complete(transport to session)
                if (sessionId != null && session != sessionId) finish("Another sender took over ${device.name}")
            }
        } else if (sessionId != null) {
            // Someone stopped the receiver app, or another app replaced it.
            finish("Casting stopped on ${device.name}")
        }
    }

    private fun handleMediaStatus(payload: JsonObject) {
        val entry = payload["status"]?.asArray()?.firstOrNull()?.asObject() ?: return
        entry["mediaSessionId"]?.jsonPrimitive?.intOrNull?.let { mediaSessionId = it }
        val media = entry["media"]?.asObject()
        val playerState = when (entry["playerState"]?.jsonPrimitive?.contentOrNull) {
            "PLAYING" -> CastPlayerState.Playing
            "PAUSED" -> CastPlayerState.Paused
            "BUFFERING" -> CastPlayerState.Buffering
            "LOADING" -> CastPlayerState.Loading
            "IDLE" -> CastPlayerState.Idle
            else -> lastStatus.playerState
        }
        val durationMs = media?.get("duration")?.jsonPrimitive?.doubleOrNull
            ?.takeIf { it.isFinite() && it > 0.0 }
            ?.let { (it * 1000).toLong() }
            ?: lastStatus.durationMs
        val mediaTracks = media?.get("tracks")?.asArray()?.mapNotNull { it.asObject() }
        val receiverTracks = mediaTracks?.let { receiverTracksOfType(it, "TEXT") } ?: lastStatus.receiverTextTracks
        val receiverAudio = mediaTracks?.let { receiverTracksOfType(it, "AUDIO") } ?: lastStatus.receiverAudioTracks
        val volume = entry["volume"]?.asObject()
        val next = lastStatus.copy(
            playerState = playerState,
            idleReason = entry["idleReason"]?.jsonPrimitive?.contentOrNull.takeIf { playerState == CastPlayerState.Idle },
            positionMs = entry["currentTime"]?.jsonPrimitive?.doubleOrNull
                ?.takeIf { it.isFinite() }
                ?.let { (it * 1000).toLong() }
                ?: lastStatus.positionMs,
            durationMs = durationMs,
            playbackRate = entry["playbackRate"]?.jsonPrimitive?.doubleOrNull?.toFloat() ?: lastStatus.playbackRate,
            volumeLevel = volume?.get("level")?.jsonPrimitive?.doubleOrNull?.toFloat() ?: receiverVolume ?: lastStatus.volumeLevel,
            muted = volume?.get("muted")?.jsonPrimitive?.booleanOrNull ?: lastStatus.muted,
            activeTrackIds = entry["activeTrackIds"]?.asArray()
                ?.mapNotNull { it.jsonPrimitive.intOrNull }
                ?: lastStatus.activeTrackIds,
            receiverTextTracks = receiverTracks,
            receiverAudioTracks = receiverAudio,
        )
        publish(next, resetClock = true)
    }

    private fun publish(status: CastMediaStatus, resetClock: Boolean) {
        lastStatus = status
        if (resetClock) lastStatusAtMs = System.currentTimeMillis()
        if (!ended) onStatus(status)
    }

    private fun finish(reason: String?) {
        if (ended) return
        ended = true
        pendingRequests.values.forEach { it.completeExceptionally(IllegalStateException(reason)) }
        pendingRequests.clear()
        if (!appReady.isDone) appReady.completeExceptionally(IllegalStateException(reason))
        channel.close()
        pollThread?.interrupt()
        log.d { "cast session ended device=${device.name} reason=$reason" }
        onEnded(reason)
    }

    private fun nextRequestId(): Int = requestIds.getAndIncrement()

    companion object {
        const val DEFAULT_MEDIA_RECEIVER_APP_ID = "CC1AD845"
        private const val LAUNCH_TIMEOUT_SECONDS = 20L
        private const val LOAD_TIMEOUT_SECONDS = 30L
        private const val STATUS_POLL_INTERVAL_MS = 4_000L

        private fun connectPayload(): String =
            buildJsonObject {
                put("type", "CONNECT")
                put("origin", buildJsonObject {})
                put("userAgent", "Nuvio Desktop")
                putJsonObject("senderInfo") {
                    put("sdkType", 2)
                    put("version", "1.0")
                    put("browserVersion", "Nuvio")
                    put("platform", 4)
                    put("connectionType", 1)
                }
            }.toString()

        internal fun buildLoadPayload(request: CastLoadRequest, requestId: Int, sessionId: String?): JsonObject =
            buildJsonObject {
                put("type", "LOAD")
                put("requestId", requestId)
                sessionId?.let { put("sessionId", it) }
                putJsonObject("media") {
                    put("contentId", request.contentUrl)
                    put("contentUrl", request.contentUrl)
                    put("contentType", request.contentType)
                    put("streamType", "BUFFERED")
                    putJsonObject("metadata") {
                        put("metadataType", 0)
                        put("title", request.title)
                        if (request.subtitle.isNotBlank()) put("subtitle", request.subtitle)
                        request.imageUrl?.takeIf { it.isNotBlank() }?.let { image ->
                            putJsonArray("images") { addJsonObject { put("url", image) } }
                        }
                    }
                    if (request.textTracks.isNotEmpty()) {
                        putJsonArray("tracks") {
                            request.textTracks.forEach { track ->
                                addJsonObject {
                                    put("trackId", track.id)
                                    put("type", "TEXT")
                                    put("subtype", "SUBTITLES")
                                    put("trackContentId", track.url)
                                    put("trackContentType", "text/vtt")
                                    put("name", track.name)
                                    track.language?.takeIf { it.isNotBlank() }?.let { put("language", it) }
                                }
                            }
                        }
                    }
                    request.textTrackStyle?.let { put("textTrackStyle", it.toJson()) }
                }
                put("autoplay", request.autoplay)
                put("currentTime", request.startPositionMs.coerceAtLeast(0L) / 1000.0)
                if (request.activeTextTrackIds.isNotEmpty()) {
                    putJsonArray("activeTrackIds") { request.activeTextTrackIds.forEach { add(JsonPrimitive(it)) } }
                }
            }

        private fun CastTextTrackStyle.toJson(): JsonObject =
            buildJsonObject {
                put("foregroundColor", foregroundColor)
                put("backgroundColor", "#00000000")
                put("fontScale", fontScale.toDouble())
                put("fontStyle", if (bold) "BOLD" else "NORMAL")
                if (edgeColor != null) {
                    put("edgeType", "OUTLINE")
                    put("edgeColor", edgeColor)
                } else {
                    put("edgeType", "DROP_SHADOW")
                    put("edgeColor", "#000000FF")
                }
            }

        internal fun mergeActiveTrackIds(current: List<Int>, replacedIds: Set<Int>, next: List<Int>): List<Int> =
            (current.filterNot { it in replacedIds } + next).distinct()

        private fun receiverTracksOfType(tracks: List<JsonObject>, type: String): List<CastReceiverTrack> =
            tracks
                .filter { it["type"]?.jsonPrimitive?.contentOrNull == type }
                .mapNotNull { track ->
                    val id = track["trackId"]?.jsonPrimitive?.intOrNull ?: return@mapNotNull null
                    CastReceiverTrack(
                        id = id,
                        name = track["name"]?.jsonPrimitive?.contentOrNull.orEmpty(),
                        language = track["language"]?.jsonPrimitive?.contentOrNull,
                    )
                }

        private fun JsonElement.asObject(): JsonObject? = this as? JsonObject
        private fun JsonElement.asArray(): JsonArray? = this as? JsonArray
    }
}
