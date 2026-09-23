package com.nuvio.app.features.player.desktop.cast

import co.touchlab.kermit.Logger
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.Locale
import java.util.concurrent.TimeUnit

/** What to convert for the receiver: the stream, from where, and which of its audio tracks. */
internal data class CastTranscodeSpec(
    val inputUrl: String,
    val headers: Map<String, String>,
    val startMs: Long,
    val audioTrackIndex: Int,
    /** Codec of the first video stream, from [CastTranscoder.probe]; decides the MP4 video tag. */
    val videoCodec: String?,
)

internal data class CastMediaProbe(
    val durationMs: Long,
    val videoCodec: String?,
)

/**
 * Converts audio Chromecasts can't decode (DTS, TrueHD, AC-3/E-AC-3 without TV passthrough)
 * with ffmpeg while casting. Video is copied untouched; the chosen audio track becomes stereo
 * AAC; the result is fragmented MP4 written to stdout, which the receiver plays as it streams.
 *
 * ffmpeg comes from -Dnuvio.ffmpeg.binary / NUVIO_FFMPEG_BINARY, a copy bundled in the app
 * (/ffmpeg/<os>-<arch>/ffmpeg[.exe], extracted once), or the PATH.
 */
internal class CastTranscoder(private val extractDir: () -> File) {
    private val log = Logger.withTag("CastTranscoder")

    @Volatile
    private var resolved: File? = null

    fun binary(): File? {
        resolved?.takeIf { it.canExecute() }?.let { return it }
        val found = runCatching { configuredBinary() ?: bundledBinary() ?: pathBinary() }
            .onFailure { error -> log.w(error) { "ffmpeg lookup failed" } }
            .getOrNull()
        resolved = found
        if (found != null) log.d { "using ffmpeg at ${found.absolutePath}" }
        return found
    }

    val isAvailable: Boolean get() = binary() != null

    /** Reads duration and video codec from ffmpeg's input summary. Blocking. */
    fun probe(inputUrl: String, headers: Map<String, String>): CastMediaProbe? {
        val ffmpeg = binary() ?: return null
        val command = buildList {
            add(ffmpeg.absolutePath)
            add("-hide_banner")
            add("-nostdin")
            addAll(inputArguments(inputUrl, headers))
            add("-i")
            add(inputUrl)
        }
        // ffmpeg exits with an error here (no output given) after printing the input summary.
        return runProcessOutput(command)?.let(::parseProbe)
    }

    /**
     * Where a conversion started at [startMs] really begins. Copied video must start on a
     * keyframe, so ffmpeg begins at the keyframe before [startMs]; that is time zero for the
     * receiver. Reads the first video packet's timestamp. Blocking.
     */
    fun keyframeStart(inputUrl: String, headers: Map<String, String>, startMs: Long): Long? {
        val ffmpeg = binary() ?: return null
        val command = buildList {
            add(ffmpeg.absolutePath)
            addAll(listOf("-hide_banner", "-nostdin", "-loglevel", "error"))
            addAll(inputArguments(inputUrl, headers))
            add("-ss")
            add(String.format(Locale.ROOT, "%.3f", startMs / 1000.0))
            add("-i")
            add(inputUrl)
            addAll(listOf("-map", "0:v:0", "-c", "copy", "-frames:v", "1", "-copyts", "-start_at_zero", "-f", "framecrc", "-"))
        }
        return runProcessOutput(command)?.let(::parseFirstPacketMs)
    }

    private fun runProcessOutput(command: List<String>): String? {
        val process = ProcessBuilder(command).redirectErrorStream(true).start()
        val output = StringBuilder()
        val reader = Thread({
            runCatching { process.inputStream.bufferedReader().forEachLine { output.appendLine(it) } }
        }, "nuvio-cast-ffmpeg-query").apply {
            isDaemon = true
            start()
        }
        if (!process.waitFor(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            process.destroyForcibly()
            return null
        }
        reader.join(1_000)
        return output.toString()
    }

    /** Starts converting; the MP4 stream is the process's stdout. */
    fun start(spec: CastTranscodeSpec): Process {
        val ffmpeg = binary() ?: error("ffmpeg is not available")
        val process = ProcessBuilder(buildArguments(ffmpeg.absolutePath, spec)).start()
        // Drain ffmpeg's log so a full stderr pipe can never stall the conversion.
        Thread({
            val tail = ArrayDeque<String>()
            runCatching {
                process.errorStream.bufferedReader().forEachLine { line ->
                    if (tail.size >= 20) tail.removeFirst()
                    tail.addLast(line)
                }
            }
            val exit = runCatching { process.waitFor() }.getOrDefault(-1)
            // Most exits are ours: the conversion is stopped on every seek, reload and stop.
            if (exit != 0) log.d { "ffmpeg exited with $exit: ${tail.joinToString(" | ")}" }
        }, "nuvio-cast-ffmpeg-log").apply {
            isDaemon = true
            start()
        }
        return process
    }

    private fun configuredBinary(): File? =
        (System.getProperty("nuvio.ffmpeg.binary")?.takeIf(String::isNotBlank)
            ?: System.getenv("NUVIO_FFMPEG_BINARY")?.takeIf(String::isNotBlank))
            ?.let(::File)
            ?.takeIf(File::isFile)

    private fun bundledBinary(): File? {
        val platform = platformDir() ?: return null
        val name = executableName()
        val resource = "/ffmpeg/$platform/$name"
        val url = CastTranscoder::class.java.getResource(resource) ?: return null
        val expectedSize = runCatching { url.openConnection().contentLengthLong }.getOrDefault(-1L)
        val dir = File(extractDir(), platform).apply { mkdirs() }
        val file = File(dir, name)
        // ffmpeg is large; extract it once per bundled build rather than on every cast.
        if (file.isFile && expectedSize > 0 && file.length() == expectedSize && file.canExecute()) return file
        val temp = File(dir, "$name.tmp")
        url.openStream().use { source -> temp.outputStream().use { target -> source.copyTo(target) } }
        runCatching {
            Files.move(temp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        }.getOrElse {
            Files.move(temp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }
        file.setExecutable(true)
        return file
    }

    private fun pathBinary(): File? =
        System.getenv("PATH").orEmpty()
            .split(File.pathSeparatorChar)
            .filter(String::isNotBlank)
            .map { File(it, executableName()) }
            .firstOrNull { it.isFile && it.canExecute() }

    companion object {
        private const val PROBE_TIMEOUT_SECONDS = 20L

        private val isWindows: Boolean
            get() = System.getProperty("os.name").orEmpty().lowercase(Locale.ROOT).contains("win")

        private fun executableName(): String = if (isWindows) "ffmpeg.exe" else "ffmpeg"

        private fun platformDir(): String? {
            val osName = System.getProperty("os.name").orEmpty().lowercase(Locale.ROOT)
            val os = when {
                osName.contains("win") -> "windows"
                osName.contains("mac") -> "macos"
                osName.contains("linux") -> "linux"
                else -> return null
            }
            val arch = when (System.getProperty("os.arch").orEmpty().lowercase(Locale.ROOT)) {
                "aarch64", "arm64" -> "arm64"
                "amd64", "x86_64", "x64" -> "amd64"
                else -> return null
            }
            return "$os-$arch"
        }

        /** Codecs the receivers decode themselves; anything else found in the label gets converted. */
        internal fun needsAudioConversion(label: String?): Boolean {
            val text = label ?: return false
            return Regex("""\b(DTS(-HD)?|TrueHD|E?-?AC-?3(-JOC)?)\b""", RegexOption.IGNORE_CASE).containsMatchIn(text)
        }

        internal fun inputArguments(inputUrl: String, headers: Map<String, String>): List<String> {
            if (!inputUrl.startsWith("http://", ignoreCase = true) && !inputUrl.startsWith("https://", ignoreCase = true)) {
                return emptyList()
            }
            return buildList {
                if (headers.isNotEmpty()) {
                    add("-headers")
                    add(headers.entries.joinToString("") { (name, value) -> "$name: $value\r\n" })
                }
                addAll(listOf("-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5"))
            }
        }

        internal fun buildArguments(ffmpeg: String, spec: CastTranscodeSpec): List<String> = buildList {
            add(ffmpeg)
            addAll(listOf("-hide_banner", "-nostdin", "-loglevel", "warning"))
            addAll(inputArguments(spec.inputUrl, spec.headers))
            if (spec.startMs > 0) {
                // Before -i: fast keyframe seek, and output timestamps restart at zero.
                add("-ss")
                add(String.format(Locale.ROOT, "%.3f", spec.startMs / 1000.0))
            }
            add("-i")
            add(spec.inputUrl)
            addAll(listOf("-map", "0:v:0", "-map", "0:a:${spec.audioTrackIndex.coerceAtLeast(0)}"))
            addAll(listOf("-c:v", "copy"))
            // HEVC in MP4 must be tagged hvc1 for Cast receivers to recognise it.
            if (spec.videoCodec.equals("hevc", ignoreCase = true)) addAll(listOf("-tag:v", "hvc1"))
            addAll(listOf("-c:a", "aac", "-ac", "2", "-b:a", "192k"))
            addAll(listOf("-sn", "-dn", "-avoid_negative_ts", "make_zero"))
            addAll(listOf("-f", "mp4", "-movflags", "frag_keyframe+empty_moov+default_base_moof"))
            add("pipe:1")
        }

        /** First packet's presentation time from framecrc output ("#tb 0: 1/1000" then "0, pts, dts, ..."). */
        internal fun parseFirstPacketMs(output: String): Long? {
            val timeBase = Regex("""#tb 0:\s*(\d+)/(\d+)""").find(output) ?: return null
            val (num, den) = timeBase.destructured
            val packet = output.lines().firstOrNull { it.startsWith("0,") } ?: return null
            val pts = packet.split(',').getOrNull(1)?.trim()?.toLongOrNull() ?: return null
            if (den.toLong() == 0L) return null
            return (pts * num.toLong() * 1000 / den.toLong()).coerceAtLeast(0L)
        }

        internal fun parseProbe(output: String): CastMediaProbe? {
            val duration = Regex("""Duration:\s*(\d+):(\d{2}):(\d{2}(?:\.\d+)?)""").find(output)?.let { match ->
                val (hours, minutes, seconds) = match.destructured
                ((hours.toLong() * 3600 + minutes.toLong() * 60) * 1000 + (seconds.toDouble() * 1000).toLong())
            }
            val videoCodec = Regex("""Stream #\d+:\d+.*?: Video: (\w+)""").find(output)?.groupValues?.get(1)
            if (duration == null && videoCodec == null) return null
            return CastMediaProbe(durationMs = duration ?: 0L, videoCodec = videoCodec)
        }
    }
}
