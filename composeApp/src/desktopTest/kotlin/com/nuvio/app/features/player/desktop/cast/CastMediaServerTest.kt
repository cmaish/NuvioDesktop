package com.nuvio.app.features.player.desktop.cast

import com.sun.net.httpserver.HttpServer
import java.io.File
import java.net.HttpURLConnection
import java.net.InetSocketAddress
import java.net.URI
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CastMediaServerTest {
    private val server = CastMediaServer()
    private var upstream: HttpServer? = null

    @AfterTest
    fun tearDown() {
        server.stop()
        upstream?.stop(0)
    }

    @Test
    fun `srt subtitles become WebVTT with the delay applied`() {
        val srt = """
            1
            00:00:01,000 --> 00:00:02,500
            Hello <i>there</i> & welcome

            2
            00:00:03,000 --> 00:00:04,000
            Second line
        """.trimIndent()

        val vtt = CastSubtitleConverter.toWebVtt(srt, "https://subs.example/movie.srt", delayMs = 500)!!

        assertTrue(vtt.startsWith("WEBVTT\n\n"))
        assertTrue("00:00:01.500 --> 00:00:03.000\nHello there &amp; welcome" in vtt, vtt)
        assertTrue("00:00:03.500 --> 00:00:04.500\nSecond line" in vtt, vtt)
    }

    @Test
    fun `negative delay drops cues that would start before zero length`() {
        val srt = "1\n00:00:00,100 --> 00:00:00,400\nGone\n\n2\n00:00:05,000 --> 00:00:06,000\nKept\n"

        val vtt = CastSubtitleConverter.toWebVtt(srt, "a.srt", delayMs = -1_000)!!

        assertTrue("Gone" !in vtt)
        assertTrue("00:00:04.000 --> 00:00:05.000\nKept" in vtt, vtt)
    }

    @Test
    fun `ass subtitles are converted`() {
        val ass = """
            [Script Info]
            Title: test

            [Events]
            Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
            Dialogue: 0,0:00:10.00,0:00:12.50,Default,,0,0,0,,{\i1}Styled{\i0}\Nline
        """.trimIndent()

        val vtt = CastSubtitleConverter.toWebVtt(ass, "episode.ass")!!

        assertTrue("00:00:10.000 --> 00:00:12.500\nStyled\nline" in vtt, vtt)
    }

    @Test
    fun `range header parsing`() {
        assertEquals(0L to 99L, CastMediaServer.parseRange("bytes=0-", 100))
        assertEquals(10L to 19L, CastMediaServer.parseRange("bytes=10-19", 100))
        assertEquals(90L to 99L, CastMediaServer.parseRange("bytes=-10", 100))
        assertEquals(50L to 99L, CastMediaServer.parseRange("bytes=50-500", 100))
        assertNull(CastMediaServer.parseRange("bytes=200-", 100))
        assertNull(CastMediaServer.parseRange(null, 100))
    }

    @Test
    fun `hls playlists are rewritten through the proxy`() {
        val playlist = """
            #EXTM3U
            #EXT-X-KEY:METHOD=AES-128,URI="key.bin"
            #EXT-X-MEDIA:TYPE=SUBTITLES,URI="subs/en.m3u8"
            #EXTINF:4.0,
            seg-1.ts
            https://other.example/seg-2.ts
        """.trimIndent()

        val rewritten = CastMediaServer.rewriteHlsPlaylist(playlist, "https://cdn.example/path/index.m3u8") { "P[$it]" }

        assertTrue("URI=\"P[https://cdn.example/path/key.bin]\"" in rewritten, rewritten)
        assertTrue("URI=\"P[https://cdn.example/path/subs/en.m3u8]\"" in rewritten, rewritten)
        assertTrue("\nP[https://cdn.example/path/seg-1.ts]\n" in rewritten, rewritten)
        assertTrue(rewritten.endsWith("P[https://other.example/seg-2.ts]"), rewritten)
        assertTrue(rewritten.startsWith("#EXTM3U\n"))
    }

    @Test
    fun `content type is guessed from the url`() {
        assertEquals("application/x-mpegurl", CastMediaServer.guessContentType("https://a/b/master.m3u8?token=1"))
        assertEquals("video/x-matroska", CastMediaServer.guessContentType("file:///movies/a.MKV"))
        assertEquals("video/mp4", CastMediaServer.guessContentType("https://a/resolve/12345"))
    }

    @Test
    fun `served subtitles carry CORS headers`() {
        server.start()
        val url = server.publishSubtitle("127.0.0.1", "WEBVTT\n\n").toLoopback()

        val connection = URI(url).toURL().openConnection() as HttpURLConnection

        assertEquals(200, connection.responseCode)
        assertEquals("*", connection.getHeaderField("Access-Control-Allow-Origin"))
        assertTrue(connection.contentType.startsWith("text/vtt"))
        assertEquals("WEBVTT\n\n", connection.inputStream.readBytes().decodeToString())
    }

    @Test
    fun `local files are served with byte ranges`() {
        server.start()
        val file = File.createTempFile("nuvio-cast", ".mp4").apply {
            deleteOnExit()
            writeBytes(ByteArray(1_000) { (it % 251).toByte() })
        }
        val url = server.publishFile("127.0.0.1", file).toLoopback()

        val connection = URI(url).toURL().openConnection() as HttpURLConnection
        connection.setRequestProperty("Range", "bytes=100-199")

        assertEquals(206, connection.responseCode)
        assertEquals("bytes 100-199/1000", connection.getHeaderField("Content-Range"))
        val body = connection.inputStream.readBytes()
        assertEquals(100, body.size)
        assertEquals((100 % 251).toByte(), body.first())
    }

    @Test
    fun `proxy forwards the stream headers and range`() {
        val seen = mutableListOf<Pair<String?, String?>>()
        upstream = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/video.mp4") { exchange ->
                seen += exchange.requestHeaders.getFirst("Referer") to exchange.requestHeaders.getFirst("Range")
                val body = "partial".encodeToByteArray()
                exchange.responseHeaders.add("Content-Type", "video/mp4")
                exchange.responseHeaders.add("Content-Range", "bytes 0-6/100")
                exchange.sendResponseHeaders(206, body.size.toLong())
                exchange.responseBody.use { it.write(body) }
            }
            start()
        }
        server.start()
        val target = "http://127.0.0.1:${upstream!!.address.port}/video.mp4"
        val url = server.publishProxy("127.0.0.1", target, mapOf("Referer" to "https://addon.example/")).toLoopback()

        val connection = URI(url).toURL().openConnection() as HttpURLConnection
        connection.setRequestProperty("Range", "bytes=0-6")

        assertEquals(206, connection.responseCode)
        assertEquals("bytes 0-6/100", connection.getHeaderField("Content-Range"))
        assertEquals("partial", connection.inputStream.readBytes().decodeToString())
        assertEquals<List<Pair<String?, String?>>>(listOf("https://addon.example/" to "bytes=0-6"), seen)
    }

    @Test
    fun `proxy rejects unknown tokens`() {
        server.start()
        val valid = server.publishProxy("127.0.0.1", "http://127.0.0.1:1/x", emptyMap()).toLoopback()
        val forged = valid.replace(Regex("/p/[0-9a-f]+/"), "/p/0000/")

        val connection = URI(forged).toURL().openConnection() as HttpURLConnection

        assertEquals(404, connection.responseCode)
    }

    // The server advertises its LAN address; tests talk to it over loopback.
    private fun String.toLoopback(): String = replace(Regex("^http://[^/:]+:"), "http://127.0.0.1:")
}
