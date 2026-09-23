package com.nuvio.app.features.player.desktop.cast

import java.io.ByteArrayOutputStream

/**
 * A CASTV2 `CastMessage` protobuf. Only the handful of fields the sender protocol uses
 * are modelled, and the wire format is encoded by hand so the desktop build does not
 * need a protobuf runtime for a six-field message.
 */
internal data class CastMessage(
    val sourceId: String,
    val destinationId: String,
    val namespace: String,
    val payloadUtf8: String? = null,
    val payloadBinary: ByteArray? = null,
) {
    fun encode(): ByteArray {
        val out = ByteArrayOutputStream()
        out.writeVarintField(FIELD_PROTOCOL_VERSION, PROTOCOL_VERSION_CASTV2_1_0)
        out.writeStringField(FIELD_SOURCE_ID, sourceId)
        out.writeStringField(FIELD_DESTINATION_ID, destinationId)
        out.writeStringField(FIELD_NAMESPACE, namespace)
        if (payloadBinary != null) {
            out.writeVarintField(FIELD_PAYLOAD_TYPE, PAYLOAD_TYPE_BINARY)
            out.writeBytesField(FIELD_PAYLOAD_BINARY, payloadBinary)
        } else {
            out.writeVarintField(FIELD_PAYLOAD_TYPE, PAYLOAD_TYPE_STRING)
            out.writeStringField(FIELD_PAYLOAD_UTF8, payloadUtf8.orEmpty())
        }
        return out.toByteArray()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is CastMessage) return false
        return sourceId == other.sourceId &&
            destinationId == other.destinationId &&
            namespace == other.namespace &&
            payloadUtf8 == other.payloadUtf8 &&
            payloadBinary.contentEquals(other.payloadBinary)
    }

    override fun hashCode(): Int {
        var result = sourceId.hashCode()
        result = 31 * result + destinationId.hashCode()
        result = 31 * result + namespace.hashCode()
        result = 31 * result + (payloadUtf8?.hashCode() ?: 0)
        result = 31 * result + (payloadBinary?.contentHashCode() ?: 0)
        return result
    }

    companion object {
        private const val FIELD_PROTOCOL_VERSION = 1
        private const val FIELD_SOURCE_ID = 2
        private const val FIELD_DESTINATION_ID = 3
        private const val FIELD_NAMESPACE = 4
        private const val FIELD_PAYLOAD_TYPE = 5
        private const val FIELD_PAYLOAD_UTF8 = 6
        private const val FIELD_PAYLOAD_BINARY = 7

        private const val WIRE_VARINT = 0
        private const val WIRE_FIXED64 = 1
        private const val WIRE_LENGTH_DELIMITED = 2
        private const val WIRE_FIXED32 = 5

        private const val PROTOCOL_VERSION_CASTV2_1_0 = 0L
        private const val PAYLOAD_TYPE_STRING = 0L
        private const val PAYLOAD_TYPE_BINARY = 1L

        fun decode(bytes: ByteArray): CastMessage {
            val reader = ProtoReader(bytes)
            var sourceId = ""
            var destinationId = ""
            var namespace = ""
            var payloadUtf8: String? = null
            var payloadBinary: ByteArray? = null
            while (!reader.isAtEnd) {
                val key = reader.readVarint()
                val field = (key ushr 3).toInt()
                when (val wireType = (key and 0x7).toInt()) {
                    WIRE_VARINT -> reader.readVarint()
                    WIRE_LENGTH_DELIMITED -> {
                        val value = reader.readLengthDelimited()
                        when (field) {
                            FIELD_SOURCE_ID -> sourceId = value.decodeToString()
                            FIELD_DESTINATION_ID -> destinationId = value.decodeToString()
                            FIELD_NAMESPACE -> namespace = value.decodeToString()
                            FIELD_PAYLOAD_UTF8 -> payloadUtf8 = value.decodeToString()
                            FIELD_PAYLOAD_BINARY -> payloadBinary = value
                        }
                    }
                    WIRE_FIXED64 -> reader.skip(8)
                    WIRE_FIXED32 -> reader.skip(4)
                    else -> throw IllegalArgumentException("Unsupported protobuf wire type $wireType")
                }
            }
            return CastMessage(
                sourceId = sourceId,
                destinationId = destinationId,
                namespace = namespace,
                payloadUtf8 = payloadUtf8,
                payloadBinary = payloadBinary,
            )
        }

        private fun ByteArrayOutputStream.writeVarint(value: Long) {
            var remaining = value
            while (true) {
                if (remaining and 0x7FL.inv() == 0L) {
                    write(remaining.toInt())
                    return
                }
                write(((remaining and 0x7F) or 0x80).toInt())
                remaining = remaining ushr 7
            }
        }

        private fun ByteArrayOutputStream.writeVarintField(field: Int, value: Long) {
            writeVarint(((field shl 3) or WIRE_VARINT).toLong())
            writeVarint(value)
        }

        private fun ByteArrayOutputStream.writeBytesField(field: Int, value: ByteArray) {
            writeVarint(((field shl 3) or WIRE_LENGTH_DELIMITED).toLong())
            writeVarint(value.size.toLong())
            write(value)
        }

        private fun ByteArrayOutputStream.writeStringField(field: Int, value: String) {
            writeBytesField(field, value.encodeToByteArray())
        }
    }
}

private class ProtoReader(private val bytes: ByteArray) {
    private var position = 0

    val isAtEnd: Boolean get() = position >= bytes.size

    fun readVarint(): Long {
        var result = 0L
        var shift = 0
        while (shift < 64) {
            require(position < bytes.size) { "Truncated protobuf varint" }
            val byte = bytes[position++].toInt()
            result = result or ((byte and 0x7F).toLong() shl shift)
            if (byte and 0x80 == 0) return result
            shift += 7
        }
        throw IllegalArgumentException("Malformed protobuf varint")
    }

    fun readLengthDelimited(): ByteArray {
        val length = readVarint()
        require(length >= 0 && length <= bytes.size - position) { "Truncated protobuf field" }
        val start = position
        position += length.toInt()
        return bytes.copyOfRange(start, position)
    }

    fun skip(count: Int) {
        require(count <= bytes.size - position) { "Truncated protobuf field" }
        position += count
    }
}
