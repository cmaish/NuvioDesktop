package com.nuvio.app.features.player.desktop.cast

import com.nuvio.app.features.player.PlayerSubtitleCueParser

/**
 * Cast receivers only render WebVTT side-loaded text tracks, so every subtitle format the
 * player understands (SRT, VTT, ASS/SSA, TTML) is normalised into WebVTT here. The current
 * subtitle delay is baked into the cue times because the receiver has no delay control.
 */
internal object CastSubtitleConverter {
    fun toWebVtt(text: String, sourceUrl: String?, delayMs: Int = 0): String? {
        val cues = PlayerSubtitleCueParser.parse(text, sourceUrl)
        if (cues.isEmpty()) return null
        return buildString {
            append("WEBVTT\n\n")
            cues.forEach { cue ->
                val start = (cue.startTimeMs + delayMs).coerceAtLeast(0L)
                val end = (cue.endTimeMs + delayMs).coerceAtLeast(0L)
                if (end <= start) return@forEach
                append(formatTimestamp(start))
                append(" --> ")
                append(formatTimestamp(end))
                append('\n')
                append(escapeCueText(cue.text))
                append("\n\n")
            }
        }
    }

    internal fun formatTimestamp(timeMs: Long): String {
        val hours = timeMs / 3_600_000L
        val minutes = (timeMs / 60_000L) % 60L
        val seconds = (timeMs / 1_000L) % 60L
        val millis = timeMs % 1_000L
        return "${hours.pad(2)}:${minutes.pad(2)}:${seconds.pad(2)}.${millis.pad(3)}"
    }

    private fun Long.pad(width: Int): String = toString().padStart(width, '0')

    private fun escapeCueText(text: String): String =
        text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            // A blank line would end the cue early.
            .lines()
            .filter { it.isNotBlank() }
            .joinToString("\n")
}
