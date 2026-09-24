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
    /**
     * Subtitle file to draw into the picture, already shifted to this stream's timeline.
     * Burning in means re-encoding the video, so it is only set while a subtitle is enabled.
     */
    val burnSubtitlesFile: File? = null,
    val subtitleStyle: CastTextTrackStyle? = null,
    /**
     * A picture-based subtitle stream of the input (PGS, VobSub, DVB) to overlay while
     * re-encoding: its position among the input's subtitle streams. Unlike text subtitles
     * these need no extraction first, so they are drawn in the same pass.
     */
    val overlaySubtitleStream: Int? = null,
    /** H.264 encoder for burned-in video, from [CastTranscoder.videoEncoder]. */
    val videoEncoder: String = SOFTWARE_H264_ENCODER,
)

internal const val SOFTWARE_H264_ENCODER = "libx264"

internal data class CastMediaProbe(
    val durationMs: Long,
    val videoCodec: String?,
    val subtitleStreams: List<CastSubtitleStream> = emptyList(),
)

/** A subtitle stream inside the media, in the order ffmpeg (and mpv) list them. */
internal data class CastSubtitleStream(
    /** Position among the input's subtitle streams: ffmpeg's `0:s:N`. */
    val index: Int,
    val codec: String,
    val language: String?,
    val title: String?,
) {
    /** Picture-based subtitles can be overlaid directly; text ones must be extracted first. */
    val isBitmap: Boolean get() = codec.lowercase(Locale.ROOT) in BITMAP_SUBTITLE_CODECS

    private companion object {
        val BITMAP_SUBTITLE_CODECS = setOf("hdmv_pgs_subtitle", "pgssub", "dvd_subtitle", "dvdsub", "dvb_subtitle", "dvbsub", "xsub")
    }
}

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

    @Volatile
    private var encoder: String? = null

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
        return runProcess(command)?.output?.let(::parseProbe)
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
        return runProcess(command)?.output?.let(::parseFirstPacketMs)
    }

    /**
     * Copies subtitle stream [streamIndex] of the input into [output] as SubRip, starting at
     * [fromMs] and keeping the original timestamps. Text subtitles are spread through the whole
     * file, so this reads the rest of it: quick from disk, slower over the network. Reports how
     * far it got (in media time) through [onProgress]. Blocking; returns false on failure or
     * when [isCancelled] turns true.
     */
    fun extractSubtitles(
        inputUrl: String,
        headers: Map<String, String>,
        streamIndex: Int,
        fromMs: Long,
        output: File,
        onProgress: (positionMs: Long) -> Unit,
        isCancelled: () -> Boolean,
    ): Boolean {
        val ffmpeg = binary() ?: return false
        val partial = File(output.parentFile, output.name + ".part")
        val process = ProcessBuilder(extractSubtitlesArguments(ffmpeg.absolutePath, inputUrl, headers, streamIndex, fromMs, partial))
            .redirectErrorStream(true)
            .start()
        val tail = ArrayDeque<String>()
        val reader = Thread({
            runCatching {
                process.inputStream.bufferedReader().forEachLine { line ->
                    if (tail.size >= 10) tail.removeFirst()
                    tail.addLast(line)
                }
            }
        }, "nuvio-cast-subtitle-extract").apply {
            isDaemon = true
            start()
        }
        // ffmpeg reports no time for subtitle-only output; the last cue written shows how far it got.
        var lastProgressCheck = 0L
        while (!process.waitFor(250, TimeUnit.MILLISECONDS)) {
            if (isCancelled()) {
                process.destroyForcibly()
                reader.join(1_000)
                partial.delete()
                return false
            }
            val now = System.currentTimeMillis()
            if (now - lastProgressCheck >= 1_000) {
                lastProgressCheck = now
                lastCueStartMs(partial)?.let(onProgress)
            }
        }
        reader.join(1_000)
        if (process.exitValue() != 0 || !partial.isFile) {
            log.w { "subtitle extraction failed (${process.exitValue()}): ${tail.joinToString(" | ")}" }
            partial.delete()
            return false
        }
        output.delete()
        return partial.renameTo(output)
    }

    private class ProcessResult(val exitCode: Int, val output: String)

    /** Runs a short ffmpeg query and collects its combined output; null on timeout. */
    private fun runProcess(command: List<String>, timeoutSeconds: Long = PROBE_TIMEOUT_SECONDS): ProcessResult? {
        val process = ProcessBuilder(command).redirectErrorStream(true).start()
        val output = StringBuilder()
        val reader = Thread({
            runCatching { process.inputStream.bufferedReader().forEachLine { output.appendLine(it) } }
        }, "nuvio-cast-ffmpeg-query").apply {
            isDaemon = true
            start()
        }
        if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
            process.destroyForcibly()
            return null
        }
        reader.join(1_000)
        return ProcessResult(process.exitValue(), output.toString())
    }

    /**
     * The fastest working H.264 encoder: the GPU's (NVIDIA, Intel, AMD, Apple) when a short test
     * encode succeeds, otherwise x264. Burning subtitles in has to encode in real time. Blocking.
     */
    fun videoEncoder(): String {
        encoder?.let { return it }
        val ffmpeg = binary() ?: return SOFTWARE_H264_ENCODER
        val found = HARDWARE_H264_ENCODERS.firstOrNull { candidate ->
            val result = runProcess(
                listOf(
                    ffmpeg.absolutePath, "-hide_banner", "-nostdin", "-loglevel", "error",
                    "-f", "lavfi", "-i", "color=black:s=640x360:r=24:d=0.5",
                    "-vf", "format=yuv420p", "-c:v", candidate, "-f", "null", "-",
                ),
                timeoutSeconds = ENCODER_TEST_TIMEOUT_SECONDS,
            )
            result != null && result.exitCode == 0
        } ?: SOFTWARE_H264_ENCODER
        log.d { "burn-in video encoder: $found" }
        encoder = found
        return found
    }

    /** Starts converting; the MP4 stream is the process's stdout. */
    fun start(spec: CastTranscodeSpec): Process {
        val ffmpeg = binary() ?: error("ffmpeg is not available")
        val process = ProcessBuilder(buildArguments(ffmpeg.absolutePath, spec))
            // The subtitles filter gets a bare file name: no filtergraph escaping of the path.
            .apply { spec.burnSubtitlesFile?.parentFile?.let(::directory) }
            .start()
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
        private const val ENCODER_TEST_TIMEOUT_SECONDS = 15L
        private val HARDWARE_H264_ENCODERS = listOf("h264_nvenc", "h264_qsv", "h264_amf", "h264_videotoolbox")
        /** Converted video is capped at 1080p: enough for the TV, and it keeps encoding real-time. */
        private const val MAX_BURN_HEIGHT = 1080
        private const val BURN_SCALE = "scale=-2:'min($MAX_BURN_HEIGHT,ih)',format=yuv420p"

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
            val burn = spec.burnSubtitlesFile
            // No -hwaccel: with a GPU driver library missing, ffmpeg aborts instead of falling back.
            if (spec.startMs > 0) {
                // Before -i: fast seek, and output timestamps restart at zero. Copied video starts
                // on the previous keyframe; re-encoded video starts exactly at startMs.
                add("-ss")
                add(String.format(Locale.ROOT, "%.3f", spec.startMs / 1000.0))
            }
            add("-i")
            add(spec.inputUrl)
            val overlay = spec.overlaySubtitleStream?.takeIf { burn == null }
            if (overlay != null) {
                // Picture subtitles are drawn at the source size, then the result is scaled.
                add("-filter_complex")
                add("[0:v:0][0:s:$overlay]overlay=eof_action=pass,$BURN_SCALE[v]")
                addAll(listOf("-map", "[v]"))
            } else {
                addAll(listOf("-map", "0:v:0"))
            }
            addAll(listOf("-map", "0:a:${spec.audioTrackIndex.coerceAtLeast(0)}"))
            if (burn != null || overlay != null) {
                if (burn != null) {
                    val subtitles = buildString {
                        append("subtitles=filename=").append(burn.name)
                        spec.subtitleStyle?.let { append(":force_style='").append(assForceStyle(it)).append('\'') }
                    }
                    add("-vf")
                    add("$BURN_SCALE,$subtitles")
                }
                addAll(listOf("-c:v", spec.videoEncoder))
                addAll(encoderArguments(spec.videoEncoder))
                addAll(listOf("-g", "48"))
            } else {
                addAll(listOf("-c:v", "copy"))
                // HEVC in MP4 must be tagged hvc1 for Cast receivers to recognise it.
                if (spec.videoCodec.equals("hevc", ignoreCase = true)) addAll(listOf("-tag:v", "hvc1"))
            }
            addAll(listOf("-c:a", "aac", "-ac", "2", "-b:a", "192k"))
            addAll(listOf("-sn", "-dn", "-avoid_negative_ts", "make_zero"))
            addAll(listOf("-f", "mp4", "-movflags", "frag_keyframe+empty_moov+default_base_moof"))
            add("pipe:1")
        }

        internal fun extractSubtitlesArguments(
            ffmpeg: String,
            inputUrl: String,
            headers: Map<String, String>,
            streamIndex: Int,
            fromMs: Long,
            output: File,
        ): List<String> = buildList {
            add(ffmpeg)
            addAll(listOf("-hide_banner", "-nostdin", "-loglevel", "error", "-nostats", "-y"))
            addAll(inputArguments(inputUrl, headers))
            if (fromMs > 0) {
                add("-ss")
                add(String.format(Locale.ROOT, "%.3f", fromMs / 1000.0))
            }
            add("-i")
            add(inputUrl)
            // -copyts keeps the cue times of the source, so one extraction serves every seek after it.
            // Written cue by cue, so progress can be read from the file while it grows.
            addAll(listOf("-copyts", "-map", "0:s:$streamIndex", "-c:s", "srt", "-flush_packets", "1", "-f", "srt", output.absolutePath))
        }

        /** Start of the last cue in a SubRip file still being written, from its last few KB. */
        internal fun lastCueStartMs(file: File): Long? {
            if (!file.isFile) return null
            val text = runCatching {
                java.io.RandomAccessFile(file, "r").use { input ->
                    val start = (input.length() - 4_096).coerceAtLeast(0L)
                    input.seek(start)
                    ByteArray((input.length() - start).toInt()).also(input::readFully).decodeToString()
                }
            }.getOrNull() ?: return null
            return lastCueStartMs(text)
        }

        internal fun lastCueStartMs(srt: String): Long? {
            val match = Regex("""(\d+):(\d{2}):(\d{2})[,.](\d{3})\s*-->""").findAll(srt).lastOrNull() ?: return null
            val (hours, minutes, seconds, millis) = match.destructured
            return ((hours.toLong() * 60 + minutes.toLong()) * 60 + seconds.toLong()) * 1000 + millis.toLong()
        }

        internal fun encoderArguments(encoder: String): List<String> = when (encoder) {
            "h264_nvenc" -> listOf("-preset", "p4", "-rc", "vbr", "-cq", "21", "-b:v", "0", "-maxrate", "12M", "-bufsize", "24M")
            "h264_qsv" -> listOf("-preset", "faster", "-global_quality", "21", "-maxrate", "12M", "-bufsize", "24M")
            "h264_amf" -> listOf("-quality", "speed", "-rc", "vbr_peak", "-b:v", "8M", "-maxrate", "12M")
            "h264_videotoolbox" -> listOf("-realtime", "1", "-b:v", "8M", "-maxrate", "12M", "-bufsize", "24M")
            else -> listOf("-preset", "veryfast", "-crf", "21", "-maxrate", "12M", "-bufsize", "24M")
        } + listOf("-profile:v", "high")

        /** libass style overrides from the player's subtitle style (colours are &HAABBGGRR, AA = transparency). */
        internal fun assForceStyle(style: CastTextTrackStyle): String = buildList {
            add("FontSize=${(18 * style.fontScale).toInt().coerceIn(8, 48)}")
            add("PrimaryColour=${assColour(style.foregroundColor)}")
            if (style.edgeColor != null) {
                add("OutlineColour=${assColour(style.edgeColor)}")
                add("BorderStyle=1")
                add("Outline=1.5")
            } else {
                add("Outline=0")
                add("Shadow=1")
            }
            add("Bold=${if (style.bold) 1 else 0}")
        }.joinToString(",")

        /** "#RRGGBBAA" (or "#RRGGBB") to ASS "&HAABBGGRR". */
        internal fun assColour(hex: String): String {
            val value = hex.removePrefix("#")
            if (value.length != 6 && value.length != 8) return "&H00FFFFFF"
            val red = value.substring(0, 2)
            val green = value.substring(2, 4)
            val blue = value.substring(4, 6)
            val alpha = value.substring(6).ifEmpty { "FF" }
            val transparency = (255 - alpha.toInt(16)).toString(16).padStart(2, '0')
            return "&H$transparency$blue$green$red".uppercase()
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
            return CastMediaProbe(durationMs = duration ?: 0L, videoCodec = videoCodec, subtitleStreams = parseSubtitleStreams(output))
        }

        /** Subtitle streams from ffmpeg's input summary, with the title from each stream's metadata. */
        internal fun parseSubtitleStreams(output: String): List<CastSubtitleStream> {
            val lines = output.lines()
            val streamLine = Regex("""^\s*Stream #0:\d+(?:\[\w+])?(?:\((\w+)\))?: (\w+): (\w+)""")
            val streams = mutableListOf<CastSubtitleStream>()
            lines.forEachIndexed { lineIndex, line ->
                val match = streamLine.find(line) ?: return@forEachIndexed
                val (language, type, codec) = match.destructured
                if (type != "Subtitle") return@forEachIndexed
                // The metadata block follows the stream line, indented deeper, until the next stream.
                val title = lines.drop(lineIndex + 1)
                    .takeWhile { !streamLine.containsMatchIn(it) && !it.trimStart().startsWith("Stream #") }
                    .firstNotNullOfOrNull { Regex("""^\s+title\s*:\s*(.+)$""").find(it)?.groupValues?.get(1)?.trim() }
                streams += CastSubtitleStream(
                    index = streams.size,
                    codec = codec,
                    language = language.takeIf { it.isNotBlank() && it != "und" },
                    title = title?.takeIf(String::isNotBlank),
                )
            }
            return streams
        }
    }
}
