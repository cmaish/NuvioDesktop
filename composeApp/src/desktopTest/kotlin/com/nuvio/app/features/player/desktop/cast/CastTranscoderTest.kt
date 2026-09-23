package com.nuvio.app.features.player.desktop.cast

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.HttpURLConnection
import java.net.URI
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CastTranscoderTest {
    private val server = CastMediaServer()

    @AfterTest
    fun tearDown() {
        server.stop()
    }

    @Test
    fun `audio Chromecasts cannot decode is converted`() {
        assertTrue(CastTranscoder.needsAudioConversion("English · 5.1 · DTS"))
        assertTrue(CastTranscoder.needsAudioConversion("English · 5.1 · DTS-HD"))
        assertTrue(CastTranscoder.needsAudioConversion("English · 7.1 · TrueHD"))
        assertTrue(CastTranscoder.needsAudioConversion("English · 5.1 · E-AC-3-JOC"))
        assertTrue(CastTranscoder.needsAudioConversion("Track 2 · AC-3"))
        assertFalse(CastTranscoder.needsAudioConversion("English · Stereo · AAC"))
        assertFalse(CastTranscoder.needsAudioConversion("English · Opus"))
        assertFalse(CastTranscoder.needsAudioConversion(null))
    }

    @Test
    fun `conversion copies video and turns the chosen track into stereo AAC fragmented MP4`() {
        val args = CastTranscoder.buildArguments(
            "ffmpeg",
            CastTranscodeSpec(
                inputUrl = "https://debrid.example/dl/abc",
                headers = mapOf("Referer" to "https://addon.example/", "User-Agent" to "Nuvio"),
                startMs = 90_500L,
                audioTrackIndex = 2,
                videoCodec = "hevc",
            ),
        )

        assertEquals("ffmpeg", args.first())
        assertEquals("pipe:1", args.last())
        assertEquals("Referer: https://addon.example/\r\nUser-Agent: Nuvio\r\n", args[args.indexOf("-headers") + 1])
        // Seeking goes before -i so it is fast and the output starts at zero.
        assertTrue(args.indexOf("-ss") < args.indexOf("-i"))
        assertEquals("90.500", args[args.indexOf("-ss") + 1])
        assertEquals("https://debrid.example/dl/abc", args[args.indexOf("-i") + 1])
        assertTrue(args.containsSequence("-map", "0:v:0"))
        assertTrue(args.containsSequence("-map", "0:a:2"))
        assertTrue(args.containsSequence("-c:v", "copy"))
        assertTrue(args.containsSequence("-tag:v", "hvc1"))
        assertTrue(args.containsSequence("-c:a", "aac"))
        assertTrue(args.containsSequence("-ac", "2"))
        assertTrue(args.containsSequence("-f", "mp4"))
        assertTrue(args[args.indexOf("-movflags") + 1].contains("frag_keyframe"))
    }

    @Test
    fun `local files take no network options and H264 keeps its tag`() {
        val args = CastTranscoder.buildArguments(
            "ffmpeg",
            CastTranscodeSpec("C:\\Movies\\film.mkv", emptyMap(), 0L, 0, "h264"),
        )

        assertFalse("-headers" in args)
        assertFalse("-reconnect" in args)
        assertFalse("-ss" in args)
        assertFalse("-tag:v" in args)
    }

    @Test
    fun `burning subtitles re-encodes the video with the subtitles filter`() {
        val args = CastTranscoder.buildArguments(
            "ffmpeg",
            CastTranscodeSpec(
                inputUrl = "https://debrid.example/dl/abc",
                headers = emptyMap(),
                startMs = 30_000L,
                audioTrackIndex = 0,
                videoCodec = "hevc",
                burnSubtitlesFile = java.io.File("/tmp/cast-subtitles/burn-1.vtt"),
                subtitleStyle = CastTextTrackStyle("#FFFF00FF", "#000000FF", 1.5f, bold = true),
                videoEncoder = "h264_nvenc",
            ),
        )

        val filter = args[args.indexOf("-vf") + 1]
        // A bare file name: ffmpeg runs in the subtitle's directory, so no path escaping is needed.
        assertTrue(filter.contains("subtitles=filename=burn-1.vtt:force_style='"), filter)
        assertTrue(filter.startsWith("scale=-2:'min(1080,ih)',format=yuv420p,"), filter)
        assertTrue(args.containsSequence("-c:v", "h264_nvenc"))
        assertFalse(args.containsSequence("-c:v", "copy"))
        assertFalse("-tag:v" in args)
        assertTrue(args.containsSequence("-c:a", "aac"))
    }

    @Test
    fun `subtitle style maps to libass overrides`() {
        assertEquals("&H0000FFFF", CastTranscoder.assColour("#FFFF00FF"))
        assertEquals("&H80FFFFFF", CastTranscoder.assColour("#FFFFFF7F"))
        assertEquals("&H00563412", CastTranscoder.assColour("#123456"))
        assertEquals(
            "FontSize=27,PrimaryColour=&H0000FFFF,OutlineColour=&H00000000,BorderStyle=1,Outline=1.5,Bold=1",
            CastTranscoder.assForceStyle(CastTextTrackStyle("#FFFF00FF", "#000000FF", 1.5f, bold = true)),
        )
        assertTrue(CastTranscoder.assForceStyle(CastTextTrackStyle("#FFFFFFFF", null, 1f, bold = false)).contains("Outline=0"))
    }

    @Test
    fun `probe output gives duration and video codec`() {
        val output = """
            Input #0, matroska,webm, from 'https://debrid.example/dl/abc':
              Duration: 01:02:03.45, start: 0.000000, bitrate: 8000 kb/s
              Stream #0:0(eng): Video: hevc (Main 10), yuv420p10le(tv), 3840x2160, 23.98 fps
              Stream #0:1(eng): Audio: dts (DTS-HD MA), 48000 Hz, 5.1(side), s32p (24 bit)
            At least one output file must be specified
        """.trimIndent()

        assertEquals(CastMediaProbe(durationMs = 3_723_450L, videoCodec = "hevc"), CastTranscoder.parseProbe(output))
        assertEquals(null, CastTranscoder.parseProbe("https://x: Server returned 403 Forbidden"))
    }

    @Test
    fun `first packet time is read from framecrc output`() {
        val output = """
            #extradata 0:       48, 0x84ad115e
            #tb 0: 1/1000
            #media_type 0: video
            0,      27917,      28000,       42,     4634, 0x12420d6a
        """.trimIndent()

        assertEquals(27_917L, CastTranscoder.parseFirstPacketMs(output))
        assertEquals(1_500L, CastTranscoder.parseFirstPacketMs("#tb 0: 1/90000\n0, 135000, 135000, 1, 1, 0x0"))
        assertEquals(null, CastTranscoder.parseFirstPacketMs("Connection refused"))
    }

    @Test
    fun `process streams are served as MP4 and each request starts a new process`() {
        var started = 0
        val url = server.publishProcessStream("127.0.0.1", "stream.mp4") {
            started++
            FakeProcess("converted-$started".encodeToByteArray())
        }.toLoopback()

        val first = URI(url).toURL().openConnection() as HttpURLConnection
        assertEquals(200, first.responseCode)
        assertEquals("video/mp4", first.contentType)
        assertEquals("converted-1", first.inputStream.readBytes().decodeToString())

        val second = URI(url).toURL().openConnection() as HttpURLConnection
        assertEquals("converted-2", second.inputStream.readBytes().decodeToString())
    }

    @Test
    fun `stopping process streams ends the running conversion`() {
        val process = FakeProcess(ByteArray(0), blockUntilDestroyed = true)
        val url = server.publishProcessStream("127.0.0.1", "stream.mp4") { process }.toLoopback()
        val reader = Thread { runCatching { URI(url).toURL().openStream().use { it.readBytes() } } }.apply { start() }
        assertTrue(process.started.await(5, TimeUnit.SECONDS))

        server.stopProcessStreams()
        reader.join(5_000)

        assertTrue(process.destroyed)
        assertFalse(reader.isAlive)
    }

    private fun List<String>.containsSequence(first: String, second: String): Boolean =
        indices.any { this[it] == first && getOrNull(it + 1) == second }

    private fun String.toLoopback(): String = replace(Regex("^http://[^/:]+:"), "http://127.0.0.1:")

    private class FakeProcess(
        private val output: ByteArray,
        private val blockUntilDestroyed: Boolean = false,
    ) : Process() {
        val started = CountDownLatch(1)
        private val released = CountDownLatch(1)

        @Volatile
        var destroyed = false

        override fun getOutputStream(): OutputStream = OutputStream.nullOutputStream()

        override fun getInputStream(): InputStream =
            if (!blockUntilDestroyed) {
                ByteArrayInputStream(output)
            } else {
                object : InputStream() {
                    override fun read(): Int {
                        started.countDown()
                        released.await()
                        return -1
                    }
                }
            }

        override fun getErrorStream(): InputStream = InputStream.nullInputStream()

        override fun waitFor(): Int {
            released.await()
            return 0
        }

        override fun exitValue(): Int = 0

        override fun destroy() {
            destroyed = true
            released.countDown()
        }

        override fun destroyForcibly(): Process {
            destroy()
            return this
        }
    }
}
