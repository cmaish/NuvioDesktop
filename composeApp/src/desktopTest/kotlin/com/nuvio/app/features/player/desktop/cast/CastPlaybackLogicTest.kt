package com.nuvio.app.features.player.desktop.cast

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CastPlaybackLogicTest {
    private val streams = listOf(
        CastSubtitleStream(0, "subrip", "eng", "English"),
        CastSubtitleStream(1, "subrip", "eng", "English SDH"),
        CastSubtitleStream(2, "hdmv_pgs_subtitle", "spa", null),
        CastSubtitleStream(3, "ass", "jpn", "Signs"),
    )

    @Test
    fun `built-in track index maps straight to the subtitle stream when the language agrees`() {
        val match = DesktopCastManager.matchEmbeddedStream(
            CastSubtitleSelection(embeddedIndex = 1, embeddedLanguage = "eng", embeddedName = "English SDH"),
            streams,
        )
        assertEquals(1, match?.index)
    }

    @Test
    fun `two and three letter language codes match`() {
        val match = DesktopCastManager.matchEmbeddedStream(
            CastSubtitleSelection(embeddedIndex = 2, embeddedLanguage = "es"),
            streams,
        )
        assertEquals(2, match?.index)
    }

    @Test
    fun `a shifted index falls back to language and title`() {
        // e.g. an added subtitle file listed first: position 0 is not the file's first stream.
        val match = DesktopCastManager.matchEmbeddedStream(
            CastSubtitleSelection(embeddedIndex = 0, embeddedLanguage = "jpn", embeddedName = "Signs · ASS"),
            streams,
        )
        assertEquals(3, match?.index)
        assertEquals(
            null,
            DesktopCastManager.matchEmbeddedStream(CastSubtitleSelection(embeddedIndex = 9, embeddedLanguage = "ger"), streams),
        )
        assertEquals(null, DesktopCastManager.matchEmbeddedStream(CastSubtitleSelection(embeddedIndex = 0), emptyList()))
    }

    @Test
    fun `only streams that stop well before their end are resumed`() {
        assertTrue(DesktopCastManager.shouldRecover(durationMs = 3_600_000L, lastPositionMs = 1_200_000L, reachedPlayback = true, attempts = 0))
        // Real end of the episode.
        assertFalse(DesktopCastManager.shouldRecover(3_600_000L, 3_590_000L, reachedPlayback = true, attempts = 0))
        // Never started: a stream the TV can't play is reported, not retried.
        assertFalse(DesktopCastManager.shouldRecover(3_600_000L, 0L, reachedPlayback = false, attempts = 0))
        // Unknown length, or retried enough.
        assertFalse(DesktopCastManager.shouldRecover(0L, 60_000L, reachedPlayback = true, attempts = 0))
        assertFalse(DesktopCastManager.shouldRecover(3_600_000L, 60_000L, reachedPlayback = true, attempts = 3))
    }

    @Test
    fun `receiver states map to on-screen phases`() {
        assertEquals(CastPhase.Playing, DesktopCastManager.phaseFor(CastMediaStatus(playerState = CastPlayerState.Playing)))
        assertEquals(CastPhase.Paused, DesktopCastManager.phaseFor(CastMediaStatus(playerState = CastPlayerState.Paused)))
        assertEquals(CastPhase.Buffering, DesktopCastManager.phaseFor(CastMediaStatus(playerState = CastPlayerState.Buffering)))
        assertEquals(CastPhase.Finished, DesktopCastManager.phaseFor(CastMediaStatus(playerState = CastPlayerState.Idle, idleReason = "FINISHED")))
        assertEquals(CastPhase.Loading, DesktopCastManager.phaseFor(CastMediaStatus(playerState = CastPlayerState.Idle)))
    }

    @Test
    fun `subtitles drawn into the video lose their markup`() {
        val srt = "1\n00:00:01,000 --> 00:00:02,000\n<i>Hello</i> {\\an8}there\n\n2\n00:00:03,000 --> 00:00:04,000\n{\\pos(10,10)}\n"
        val vtt = CastSubtitleConverter.toWebVtt(srt, "track.srt", plainText = true)!!
        assertTrue(vtt.contains("Hello there"), vtt)
        assertFalse(vtt.contains("&lt;"), vtt)
        // A cue left empty by removing its markup is dropped.
        assertFalse(vtt.contains("00:00:03.000"), vtt)
    }
}
