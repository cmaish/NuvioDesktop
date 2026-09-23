package com.nuvio.app.features.player.desktop.cast

import co.touchlab.kermit.Logger
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.io.RandomAccessFile
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.URI
import java.net.URLDecoder
import java.net.URLEncoder
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * A small LAN HTTP server that makes content reachable for a Cast receiver:
 *
 * - converted WebVTT subtitles (receivers require CORS headers on text tracks),
 * - local files such as downloads,
 * - streams the receiver cannot fetch itself, either because they need request headers
 *   (addon proxy headers) or because they live on this machine's loopback (P2P engine).
 *   HLS playlists are rewritten so every segment and variant goes through the proxy too.
 *
 * Every route is keyed by a random token, so the server is not an open proxy. It starts on the
 * first published resource and stops when the cast session ends.
 */
internal class CastMediaServer {
    private val log = Logger.withTag("CastMediaServer")
    private val subtitles = ConcurrentHashMap<String, String>()
    private val files = ConcurrentHashMap<String, File>()
    private val proxyScopes = ConcurrentHashMap<String, Map<String, String>>()
    private var server: HttpServer? = null
    private var executor: ExecutorService? = null
    private val httpClient: OkHttpClient by lazy {
        OkHttpClient.Builder()
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS)
            .followRedirects(true)
            .followSslRedirects(true)
            .build()
    }

    @Synchronized
    fun start() {
        if (server != null) return
        val pool = Executors.newCachedThreadPool { runnable ->
            Thread(runnable, "nuvio-cast-http").apply { isDaemon = true }
        }
        val created = HttpServer.create(InetSocketAddress(0), 32)
        created.createContext("/") { exchange ->
            try {
                handle(exchange)
            } catch (error: Throwable) {
                log.d { "cast http request failed: ${error.message}" }
                runCatching { exchange.sendResponseHeaders(500, -1) }
            } finally {
                exchange.close()
            }
        }
        created.executor = pool
        created.start()
        server = created
        executor = pool
        log.d { "cast media server listening on ${created.address.port}" }
    }

    @Synchronized
    fun stop() {
        server?.stop(0)
        server = null
        executor?.shutdownNow()
        executor = null
        subtitles.clear()
        files.clear()
        proxyScopes.clear()
    }

    /** Base URL reachable from [receiverHost], using the local address that routes to it. */
    fun baseUrlFor(receiverHost: String): String {
        val port = server?.address?.port ?: error("Cast media server is not running")
        val local = localAddressFor(receiverHost)
        return "http://${local.hostAddress}:$port"
    }

    fun publishSubtitle(receiverHost: String, webVtt: String): String {
        start()
        val token = newToken()
        subtitles[token] = webVtt
        return "${baseUrlFor(receiverHost)}/s/$token.vtt"
    }

    fun publishFile(receiverHost: String, file: File): String {
        start()
        val token = newToken()
        files[token] = file
        return "${baseUrlFor(receiverHost)}/f/$token/${file.name.urlEncode()}"
    }

    fun publishProxy(receiverHost: String, url: String, headers: Map<String, String>): String {
        start()
        val token = newToken()
        proxyScopes[token] = headers
        return proxyUrl(baseUrlFor(receiverHost), token, url)
    }

    private fun proxyUrl(base: String, token: String, url: String): String {
        val name = runCatching { URI(url).path.substringAfterLast('/') }.getOrNull()
            ?.takeIf { it.isNotBlank() }
            ?: "media"
        return "$base/p/$token/${name.urlEncode()}?u=${url.urlEncode()}"
    }

    private fun handle(exchange: HttpExchange) {
        exchange.responseHeaders.apply {
            add("Access-Control-Allow-Origin", "*")
            add("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
            add("Access-Control-Allow-Headers", "Content-Type, Range, Accept-Encoding, Origin")
            add("Access-Control-Expose-Headers", "Content-Length, Content-Range, Accept-Ranges, Content-Type")
        }
        val method = exchange.requestMethod.uppercase()
        if (method == "OPTIONS") {
            exchange.sendResponseHeaders(204, -1)
            return
        }
        if (method != "GET" && method != "HEAD") {
            exchange.sendResponseHeaders(405, -1)
            return
        }
        val segments = exchange.requestURI.rawPath.trim('/').split('/')
        val token = segments.getOrNull(1)?.substringBefore('.').orEmpty()
        when (segments.firstOrNull()) {
            "s" -> serveSubtitle(exchange, token)
            "f" -> serveFile(exchange, token)
            "p" -> serveProxy(exchange, token)
            else -> exchange.sendResponseHeaders(404, -1)
        }
    }

    private fun serveSubtitle(exchange: HttpExchange, token: String) {
        val body = subtitles[token]?.encodeToByteArray() ?: run {
            exchange.sendResponseHeaders(404, -1)
            return
        }
        exchange.responseHeaders.add("Content-Type", "text/vtt; charset=utf-8")
        if (exchange.requestMethod.equals("HEAD", ignoreCase = true)) {
            exchange.responseHeaders.add("Content-Length", body.size.toString())
            exchange.sendResponseHeaders(200, -1)
            return
        }
        exchange.sendResponseHeaders(200, body.size.toLong())
        exchange.responseBody.use { it.write(body) }
    }

    private fun serveFile(exchange: HttpExchange, token: String) {
        val file = files[token]?.takeIf { it.isFile } ?: run {
            exchange.sendResponseHeaders(404, -1)
            return
        }
        val total = file.length()
        val range = parseRange(exchange.requestHeaders.getFirst("Range"), total)
        exchange.responseHeaders.add("Content-Type", guessContentType(file.name))
        exchange.responseHeaders.add("Accept-Ranges", "bytes")
        val (start, end) = range ?: (0L to total - 1)
        val length = (end - start + 1).coerceAtLeast(0L)
        val status = if (range != null) 206 else 200
        if (range != null) exchange.responseHeaders.add("Content-Range", "bytes $start-$end/$total")
        if (exchange.requestMethod.equals("HEAD", ignoreCase = true)) {
            exchange.responseHeaders.add("Content-Length", length.toString())
            exchange.sendResponseHeaders(status, -1)
            return
        }
        exchange.sendResponseHeaders(status, if (length == 0L) -1 else length)
        if (length == 0L) return
        RandomAccessFile(file, "r").use { input ->
            input.seek(start)
            exchange.responseBody.use { output ->
                val buffer = ByteArray(64 * 1024)
                var remaining = length
                while (remaining > 0) {
                    val read = input.read(buffer, 0, minOf(buffer.size.toLong(), remaining).toInt())
                    if (read < 0) break
                    output.write(buffer, 0, read)
                    remaining -= read
                }
            }
        }
    }

    private fun serveProxy(exchange: HttpExchange, token: String) {
        val headers = proxyScopes[token]
        val target = exchange.requestURI.rawQuery
            ?.split('&')
            ?.firstOrNull { it.startsWith("u=") }
            ?.removePrefix("u=")
            ?.let { URLDecoder.decode(it, Charsets.UTF_8) }
        if (headers == null || target == null || !target.isHttpUrl()) {
            exchange.sendResponseHeaders(404, -1)
            return
        }
        val isHead = exchange.requestMethod.equals("HEAD", ignoreCase = true)
        val request = Request.Builder()
            .url(target)
            .apply {
                headers.forEach { (name, value) -> header(name, value) }
                exchange.requestHeaders.getFirst("Range")?.let { header("Range", it) }
                if (isHead) head()
            }
            .build()
        httpClient.newCall(request).execute().use { response ->
            val contentType = response.header("Content-Type").orEmpty()
            val finalUrl = response.request.url.toString()
            if (!isHead && response.isSuccessful && isHlsPlaylist(finalUrl, contentType)) {
                val playlist = response.body?.string().orEmpty()
                val base = "http://${exchange.requestHeaders.getFirst("Host") ?: exchange.localAddress.let { "${it.address.hostAddress}:${it.port}" }}"
                val body = rewriteHlsPlaylist(playlist, finalUrl) { proxyUrl(base, token, it) }.encodeToByteArray()
                exchange.responseHeaders.add("Content-Type", "application/vnd.apple.mpegurl")
                exchange.sendResponseHeaders(response.code, body.size.toLong())
                exchange.responseBody.use { it.write(body) }
                return
            }
            listOf("Content-Type", "Content-Range", "Accept-Ranges", "Last-Modified", "ETag").forEach { name ->
                response.header(name)?.let { exchange.responseHeaders.add(name, it) }
            }
            val contentLength = response.header("Content-Length")?.toLongOrNull()
            if (isHead) {
                contentLength?.let { exchange.responseHeaders.add("Content-Length", it.toString()) }
                exchange.sendResponseHeaders(response.code, -1)
                return
            }
            val body = response.body
            val length = contentLength ?: 0L
            exchange.sendResponseHeaders(response.code, if (body == null || contentLength == 0L) -1 else length)
            if (body == null || contentLength == 0L) return
            body.byteStream().use { input ->
                exchange.responseBody.use { output -> input.copyTo(output, 64 * 1024) }
            }
        }
    }

    private fun newToken(): String = UUID.randomUUID().toString().replace("-", "")

    companion object {
        internal fun localAddressFor(receiverHost: String): InetAddress {
            val routed = runCatching {
                DatagramSocket().use { socket ->
                    socket.connect(InetSocketAddress(receiverHost, CastChannel.DEFAULT_PORT))
                    socket.localAddress
                }
            }.getOrNull()
            if (routed != null && !routed.isAnyLocalAddress && !routed.isLoopbackAddress) return routed
            return runCatching {
                java.net.NetworkInterface.getNetworkInterfaces().toList()
                    .filter { it.isUp && !it.isLoopback }
                    .flatMap { it.inetAddresses.toList() }
                    .firstOrNull { it is Inet4Address && it.isSiteLocalAddress }
            }.getOrNull() ?: InetAddress.getLocalHost()
        }

        internal fun parseRange(header: String?, total: Long): Pair<Long, Long>? {
            if (header.isNullOrBlank() || total <= 0L) return null
            val spec = header.trim().removePrefix("bytes=").substringBefore(',').trim()
            val startText = spec.substringBefore('-').trim()
            val endText = spec.substringAfter('-', "").trim()
            return when {
                startText.isEmpty() -> {
                    val suffix = endText.toLongOrNull() ?: return null
                    (total - suffix).coerceAtLeast(0L) to total - 1
                }
                else -> {
                    val start = startText.toLongOrNull() ?: return null
                    if (start >= total) return null
                    val end = endText.toLongOrNull()?.coerceAtMost(total - 1) ?: (total - 1)
                    if (end < start) null else start to end
                }
            }
        }

        internal fun isHlsPlaylist(url: String, contentType: String): Boolean {
            val type = contentType.lowercase()
            return type.contains("mpegurl") ||
                url.substringBefore('?').substringBefore('#').lowercase().endsWith(".m3u8")
        }

        internal fun rewriteHlsPlaylist(playlist: String, playlistUrl: String, proxy: (String) -> String): String {
            val base = URI(playlistUrl)
            val uriAttribute = Regex("""URI="([^"]+)"""")
            return playlist.lines().joinToString("\n") { rawLine ->
                val line = rawLine.trim()
                when {
                    line.isEmpty() -> rawLine
                    line.startsWith("#") -> uriAttribute.replace(rawLine) { match ->
                        val resolved = runCatching { base.resolve(match.groupValues[1].trim()).toString() }.getOrNull()
                        if (resolved == null || !resolved.isHttpUrl()) match.value else "URI=\"${proxy(resolved)}\""
                    }
                    else -> {
                        val resolved = runCatching { base.resolve(line).toString() }.getOrNull()
                        if (resolved == null || !resolved.isHttpUrl()) rawLine else proxy(resolved)
                    }
                }
            }
        }

        internal fun guessContentType(nameOrUrl: String): String {
            val path = nameOrUrl.substringBefore('?').substringBefore('#').lowercase()
            return when {
                path.endsWith(".m3u8") -> "application/x-mpegurl"
                path.endsWith(".mpd") -> "application/dash+xml"
                path.endsWith(".ism") || path.endsWith("/manifest") -> "application/vnd.ms-sstr+xml"
                path.endsWith(".webm") -> "video/webm"
                path.endsWith(".mkv") -> "video/x-matroska"
                path.endsWith(".ts") -> "video/mp2t"
                path.endsWith(".mov") -> "video/quicktime"
                path.endsWith(".mp3") -> "audio/mpeg"
                path.endsWith(".m4a") || path.endsWith(".aac") -> "audio/mp4"
                else -> "video/mp4"
            }
        }

        private fun String.isHttpUrl(): Boolean =
            startsWith("http://", ignoreCase = true) || startsWith("https://", ignoreCase = true)

        private fun String.urlEncode(): String = URLEncoder.encode(this, Charsets.UTF_8).replace("+", "%20")
    }
}
