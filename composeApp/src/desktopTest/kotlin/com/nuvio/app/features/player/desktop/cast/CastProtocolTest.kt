package com.nuvio.app.features.player.desktop.cast

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.ByteArrayOutputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CastProtocolTest {
    @Test
    fun `cast message round trips through the protobuf encoding`() {
        val message = CastMessage(
            sourceId = "sender-0",
            destinationId = "receiver-0",
            namespace = CastNamespaces.RECEIVER,
            payloadUtf8 = """{"type":"LAUNCH","appId":"CC1AD845","requestId":1}""",
        )

        assertEquals(message, CastMessage.decode(message.encode()))
    }

    @Test
    fun `cast message encoding matches the protobuf wire format`() {
        val bytes = CastMessage(sourceId = "a", destinationId = "b", namespace = "n", payloadUtf8 = "{}").encode()

        val expected = byteArrayOf(
            0x08, 0x00, // protocol_version = CASTV2_1_0
            0x12, 0x01, 'a'.code.toByte(), // source_id
            0x1A, 0x01, 'b'.code.toByte(), // destination_id
            0x22, 0x01, 'n'.code.toByte(), // namespace
            0x28, 0x00, // payload_type = STRING
            0x32, 0x02, '{'.code.toByte(), '}'.code.toByte(), // payload_utf8
        )
        assertTrue(expected.contentEquals(bytes), bytes.joinToString { "%02x".format(it) })
    }

    @Test
    fun `cast message decoding skips unknown fields`() {
        val known = CastMessage(sourceId = "s", destinationId = "d", namespace = "n", payloadUtf8 = "{}").encode()
        // Field 15, varint 300, then a fixed32 field 16.
        val extra = byteArrayOf(0x78, 0xAC.toByte(), 0x02, 0x85.toByte(), 0x01, 1, 2, 3, 4)

        val decoded = CastMessage.decode(known + extra)

        assertEquals("s", decoded.sourceId)
        assertEquals("{}", decoded.payloadUtf8)
    }

    @Test
    fun `load payload carries the side loaded subtitle as an active WebVTT track`() {
        val payload = CastSession.buildLoadPayload(
            CastLoadRequest(
                contentUrl = "https://cdn.example/movie.mp4",
                contentType = "video/mp4",
                title = "Movie",
                subtitle = "S1E2",
                imageUrl = "https://img.example/poster.jpg",
                startPositionMs = 90_500L,
                autoplay = true,
                textTracks = listOf(CastTextTrack(1, "http://192.168.1.2:4000/s/abc.vtt", "English", "en")),
                activeTextTrackIds = listOf(1),
                textTrackStyle = CastTextTrackStyle("#FFFFFFFF", "#000000FF", 1f, bold = false),
            ),
            requestId = 7,
            sessionId = "session-1",
        )

        assertEquals("LOAD", payload["type"]!!.jsonPrimitive.content)
        assertEquals(7, payload["requestId"]!!.jsonPrimitive.int)
        assertEquals(90.5, payload["currentTime"]!!.jsonPrimitive.content.toDouble())
        val media = payload["media"]!!.jsonObject
        assertEquals("https://cdn.example/movie.mp4", media["contentId"]!!.jsonPrimitive.content)
        val track = media["tracks"]!!.jsonArray.single().jsonObject
        assertEquals("text/vtt", track["trackContentType"]!!.jsonPrimitive.content)
        assertEquals("TEXT", track["type"]!!.jsonPrimitive.content)
        assertEquals("en", track["language"]!!.jsonPrimitive.content)
        assertEquals(listOf(1), payload["activeTrackIds"]!!.jsonArray.map { it.jsonPrimitive.int })
        assertEquals("OUTLINE", media["textTrackStyle"]!!.jsonObject["edgeType"]!!.jsonPrimitive.content)
        // Payload must be valid JSON for the receiver.
        Json.parseToJsonElement(payload.toString())
    }

    @Test
    fun `mdns response with name compression resolves a video capable device`() {
        val response = MdnsResponseBuilder()
            .ptr("_googlecast._tcp.local", "Chromecast-abc123._googlecast._tcp.local")
            .srv("Chromecast-abc123._googlecast._tcp.local", 8009, "abc123.local")
            .txt(
                "Chromecast-abc123._googlecast._tcp.local",
                listOf("id=abc123", "md=Chromecast", "fn=Living Room TV", "ca=201221"),
            )
            .a("abc123.local", byteArrayOf(192.toByte(), 168.toByte(), 1, 50))
            .build()

        val devices = CastDiscovery.parseResponse(response, sourceAddress = "192.168.1.99")

        assertEquals(listOf(CastDevice("abc123", "Living Room TV", "Chromecast", "192.168.1.50", 8009)), devices)
    }

    @Test
    fun `mdns response ignores audio only speakers`() {
        val response = MdnsResponseBuilder()
            .ptr("_googlecast._tcp.local", "Nest-Mini-1._googlecast._tcp.local")
            .srv("Nest-Mini-1._googlecast._tcp.local", 8009, "nest.local")
            .txt("Nest-Mini-1._googlecast._tcp.local", listOf("id=n1", "fn=Kitchen", "ca=199172"))
            .build()

        assertTrue(CastDiscovery.parseResponse(response, sourceAddress = "192.168.1.60").isEmpty())
    }

    @Test
    fun `mdns response without an address record falls back to the sender address`() {
        val response = MdnsResponseBuilder()
            .ptr("_googlecast._tcp.local", "TV._googlecast._tcp.local")
            .txt("TV._googlecast._tcp.local", listOf("id=tv", "fn=Bedroom"))
            .build()

        val device = CastDiscovery.parseResponse(response, sourceAddress = "10.0.0.8").single()

        assertEquals("10.0.0.8", device.host)
        assertEquals(CastChannel.DEFAULT_PORT, device.port)
    }

    @Test
    fun `mdns query asks for the googlecast service`() {
        val query = CastDiscovery.buildQuery()

        assertEquals(1, query[5].toInt())
        assertFalse(query.decodeToString().indexOf("_googlecast") < 0)
    }

    @Test
    fun `local only hosts are detected for proxying`() {
        assertTrue(DesktopCastManager.isLocalOnlyHost("http://127.0.0.1:8090/stream/file.mkv"))
        assertTrue(DesktopCastManager.isLocalOnlyHost("http://localhost:8090/play"))
        assertFalse(DesktopCastManager.isLocalOnlyHost("http://192.168.1.20:8090/play"))
    }
}

/** Builds mDNS answers the way Cast devices send them: later names compressed against earlier ones. */
private class MdnsResponseBuilder {
    private val out = ByteArrayOutputStream()
    private val nameOffsets = HashMap<String, Int>()
    private var records = 0

    init {
        out.write(ByteArray(12))
    }

    fun ptr(name: String, target: String) = record(name, 12) { writeName(target, it) }

    fun srv(name: String, port: Int, target: String) = record(name, 33) { base ->
        write(byteArrayOf(0, 0, 0, 0, (port shr 8).toByte(), port.toByte()))
        writeName(target, base + 6)
    }

    fun txt(name: String, entries: List<String>) = record(name, 16) {
        entries.forEach { entry ->
            val bytes = entry.encodeToByteArray()
            write(bytes.size)
            write(bytes)
        }
    }

    fun a(name: String, address: ByteArray) = record(name, 1) { write(address) }

    fun build(): ByteArray {
        val bytes = out.toByteArray()
        bytes[2] = 0x84.toByte()
        bytes[6] = (records shr 8).toByte()
        bytes[7] = records.toByte()
        return bytes
    }

    private fun record(name: String, type: Int, rdata: ByteArrayOutputStream.(base: Int) -> Unit): MdnsResponseBuilder {
        out.writeName(name, out.size())
        out.write(byteArrayOf(0, type.toByte(), 0x80.toByte(), 1, 0, 0, 0x11, 0x94.toByte()))
        val lengthAt = out.size()
        out.write(byteArrayOf(0, 0))
        val data = ByteArrayOutputStream()
        data.rdata(lengthAt + 2)
        val rdataBytes = data.toByteArray()
        out.write(rdataBytes)
        val bytes = out.toByteArray()
        bytes[lengthAt] = (rdataBytes.size shr 8).toByte()
        bytes[lengthAt + 1] = rdataBytes.size.toByte()
        out.reset()
        out.write(bytes)
        records++
        return this
    }

    private fun ByteArrayOutputStream.writeName(name: String, absoluteOffset: Int) {
        val labels = name.split('.')
        var written = 0
        for (index in labels.indices) {
            val suffix = labels.drop(index).joinToString(".")
            val pointer = nameOffsets[suffix]
            if (pointer != null) {
                write(0xC0 or (pointer shr 8))
                write(pointer and 0xFF)
                return
            }
            nameOffsets[suffix] = absoluteOffset + written
            val bytes = labels[index].encodeToByteArray()
            write(bytes.size)
            write(bytes)
            written += 1 + bytes.size
        }
        write(0)
    }
}
