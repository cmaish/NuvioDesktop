package com.nuvio.app.features.player.desktop.cast

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocket
import javax.net.ssl.X509TrustManager
import kotlin.concurrent.Volatile

internal object CastNamespaces {
    const val CONNECTION = "urn:x-cast:com.google.cast.tp.connection"
    const val HEARTBEAT = "urn:x-cast:com.google.cast.tp.heartbeat"
    const val RECEIVER = "urn:x-cast:com.google.cast.receiver"
    const val MEDIA = "urn:x-cast:com.google.cast.media"
}

/**
 * One TLS connection to a Cast device (port 8009) speaking length-prefixed CastMessage frames.
 *
 * Cast devices present a certificate chained to Google's device CA, not a public CA, so the
 * socket accepts any certificate — the same approach every third-party Cast sender takes.
 * The channel only ever carries playback commands for media the user already chose to cast.
 */
internal class CastChannel(
    private val host: String,
    private val port: Int,
    private val onMessage: (namespace: String, sourceId: String, payload: JsonObject) -> Unit,
    private val onClosed: (Throwable?) -> Unit,
) {
    private val log = Logger.withTag("CastChannel")
    private val closed = AtomicBoolean(false)
    private val writeLock = Any()
    private var socket: SSLSocket? = null
    private var output: DataOutputStream? = null
    private var heartbeat: ScheduledExecutorService? = null

    @Volatile
    private var lastInboundAtMs: Long = System.currentTimeMillis()

    fun connect(timeoutMs: Int = CONNECT_TIMEOUT_MS) {
        val plain = Socket()
        plain.connect(InetSocketAddress(host, port), timeoutMs)
        plain.tcpNoDelay = true
        val tls = trustAllContext().socketFactory.createSocket(plain, host, port, true) as SSLSocket
        tls.soTimeout = 0
        tls.startHandshake()
        socket = tls
        output = DataOutputStream(tls.outputStream)
        lastInboundAtMs = System.currentTimeMillis()

        Thread({ readLoop(DataInputStream(tls.inputStream)) }, "nuvio-cast-read").apply {
            isDaemon = true
            start()
        }
        heartbeat = Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "nuvio-cast-heartbeat").apply { isDaemon = true }
        }.also { executor ->
            executor.scheduleAtFixedRate({ tickHeartbeat() }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS)
        }
    }

    val isOpen: Boolean get() = !closed.get() && socket != null

    fun send(namespace: String, destinationId: String, payload: String, sourceId: String = SENDER_ID) {
        val frame = CastMessage(
            sourceId = sourceId,
            destinationId = destinationId,
            namespace = namespace,
            payloadUtf8 = payload,
        ).encode()
        val stream = output ?: throw IllegalStateException("Cast channel is not connected")
        synchronized(writeLock) {
            stream.writeInt(frame.size)
            stream.write(frame)
            stream.flush()
        }
    }

    fun close() {
        closeWith(null)
    }

    private fun tickHeartbeat() {
        if (closed.get()) return
        if (System.currentTimeMillis() - lastInboundAtMs > HEARTBEAT_TIMEOUT_MS) {
            closeWith(IllegalStateException("Cast device stopped responding"))
            return
        }
        runCatching { send(CastNamespaces.HEARTBEAT, RECEIVER_ID, """{"type":"PING"}""") }
            .onFailure { error -> closeWith(error) }
    }

    private fun readLoop(input: DataInputStream) {
        try {
            while (!closed.get()) {
                val length = input.readInt()
                if (length <= 0 || length > MAX_FRAME_BYTES) {
                    throw IllegalStateException("Invalid cast frame length $length")
                }
                val bytes = ByteArray(length)
                input.readFully(bytes)
                lastInboundAtMs = System.currentTimeMillis()
                val message = runCatching { CastMessage.decode(bytes) }
                    .onFailure { error -> log.w(error) { "dropping undecodable cast frame" } }
                    .getOrNull() ?: continue
                val payloadText = message.payloadUtf8 ?: continue
                val payload = runCatching { json.parseToJsonElement(payloadText).jsonObject }.getOrNull() ?: continue
                val type = payload["type"]?.jsonPrimitive?.contentOrNull
                if (message.namespace == CastNamespaces.HEARTBEAT) {
                    if (type == "PING") {
                        runCatching { send(CastNamespaces.HEARTBEAT, message.sourceId, """{"type":"PONG"}""") }
                    }
                    continue
                }
                runCatching { onMessage(message.namespace, message.sourceId, payload) }
                    .onFailure { error -> log.w(error) { "cast message handler failed ns=${message.namespace} type=$type" } }
            }
        } catch (error: Throwable) {
            closeWith(if (closed.get()) null else error)
        }
    }

    private fun closeWith(error: Throwable?) {
        if (!closed.compareAndSet(false, true)) return
        heartbeat?.shutdownNow()
        heartbeat = null
        runCatching { socket?.close() }
        socket = null
        output = null
        if (error != null) log.w { "cast channel closed: ${error.message}" }
        onClosed(error)
    }

    companion object {
        const val SENDER_ID = "sender-0"
        const val RECEIVER_ID = "receiver-0"
        const val DEFAULT_PORT = 8009
        private const val CONNECT_TIMEOUT_MS = 8_000
        private const val HEARTBEAT_INTERVAL_MS = 5_000L
        private const val HEARTBEAT_TIMEOUT_MS = 20_000L
        private const val MAX_FRAME_BYTES = 1 shl 20
        private val json = Json { ignoreUnknownKeys = true; isLenient = true }

        private fun trustAllContext(): SSLContext {
            val trustAll = object : X509TrustManager {
                override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?) = Unit
                override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?) = Unit
                override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()
            }
            return SSLContext.getInstance("TLS").apply {
                init(null, arrayOf(trustAll), SecureRandom())
            }
        }
    }
}
