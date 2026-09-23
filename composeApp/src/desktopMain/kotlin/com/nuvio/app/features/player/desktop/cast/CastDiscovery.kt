package com.nuvio.app.features.player.desktop.cast

import co.touchlab.kermit.Logger
import java.io.ByteArrayOutputStream
import java.net.DatagramPacket
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.MulticastSocket
import java.net.NetworkInterface
import java.net.SocketTimeoutException

internal data class CastDevice(
    val id: String,
    val name: String,
    val model: String,
    val host: String,
    val port: Int = CastChannel.DEFAULT_PORT,
)

/**
 * Finds Cast devices on the local network with a plain mDNS query for `_googlecast._tcp.local`.
 *
 * The query goes out from an ephemeral port (a "legacy unicast" query, answered directly to us)
 * with the unicast-response bit set, and a best-effort listener on 5353 also catches devices that
 * answer by multicast. Both work alongside Bonjour/Avahi already running on the machine.
 */
internal object CastDiscovery {
    private val log = Logger.withTag("CastDiscovery")
    private const val SERVICE = "_googlecast._tcp.local"
    private val MDNS_GROUP: InetAddress = InetAddress.getByName("224.0.0.251")
    private const val MDNS_PORT = 5353
    private val QUERY_SCHEDULE_MS = longArrayOf(0L, 900L, 2_500L)

    fun discover(
        timeoutMs: Long,
        isCancelled: () -> Boolean,
        onDevice: (CastDevice) -> Unit,
    ) {
        val interfaces = multicastInterfaces()
        val query = buildQuery()
        val seen = HashMap<String, CastDevice>()
        val seenLock = Any()
        val report: (List<CastDevice>) -> Unit = { devices ->
            devices.forEach { device ->
                val isNew = synchronized(seenLock) {
                    val previous = seen[device.id]
                    if (previous == device) false else {
                        seen[device.id] = device
                        true
                    }
                }
                if (isNew) onDevice(device)
            }
        }

        val querySocket = MulticastSocket(0).apply { soTimeout = 250 }
        val listenSocket = runCatching {
            MulticastSocket(null as java.net.SocketAddress?).apply {
                reuseAddress = true
                bind(InetSocketAddress(MDNS_PORT))
                soTimeout = 250
                interfaces.forEach { networkInterface ->
                    runCatching { joinGroup(InetSocketAddress(MDNS_GROUP, MDNS_PORT), networkInterface) }
                }
            }
        }.onFailure { error -> log.d { "mDNS 5353 listener unavailable: ${error.message}" } }.getOrNull()

        val deadline = System.currentTimeMillis() + timeoutMs
        val receivers = listOfNotNull(querySocket, listenSocket).map { socket ->
            Thread({ receiveLoop(socket, deadline, isCancelled, report) }, "nuvio-cast-mdns").apply {
                isDaemon = true
                start()
            }
        }
        try {
            val startedAt = System.currentTimeMillis()
            for (offset in QUERY_SCHEDULE_MS) {
                val wait = startedAt + offset - System.currentTimeMillis()
                if (wait > 0) Thread.sleep(wait)
                if (isCancelled() || System.currentTimeMillis() >= deadline) break
                sendQuery(querySocket, query, interfaces)
            }
            receivers.forEach { it.join((deadline - System.currentTimeMillis()).coerceAtLeast(1L)) }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        } finally {
            runCatching { querySocket.close() }
            runCatching { listenSocket?.close() }
        }
    }

    private fun sendQuery(socket: MulticastSocket, query: ByteArray, interfaces: List<NetworkInterface>) {
        val packet = DatagramPacket(query, query.size, MDNS_GROUP, MDNS_PORT)
        if (interfaces.isEmpty()) {
            runCatching { socket.send(packet) }
            return
        }
        interfaces.forEach { networkInterface ->
            runCatching {
                socket.networkInterface = networkInterface
                socket.send(packet)
            }.onFailure { error -> log.d { "mDNS query on ${networkInterface.name} failed: ${error.message}" } }
        }
    }

    private fun receiveLoop(
        socket: MulticastSocket,
        deadline: Long,
        isCancelled: () -> Boolean,
        report: (List<CastDevice>) -> Unit,
    ) {
        val buffer = ByteArray(9_000)
        while (!isCancelled() && System.currentTimeMillis() < deadline && !socket.isClosed) {
            val packet = DatagramPacket(buffer, buffer.size)
            try {
                socket.receive(packet)
            } catch (_: SocketTimeoutException) {
                continue
            } catch (_: Exception) {
                return
            }
            val devices = runCatching {
                parseResponse(packet.data.copyOf(packet.length), packet.address?.hostAddress)
            }.getOrDefault(emptyList())
            if (devices.isNotEmpty()) report(devices)
        }
    }

    private fun multicastInterfaces(): List<NetworkInterface> =
        runCatching {
            NetworkInterface.getNetworkInterfaces().toList().filter { networkInterface ->
                runCatching {
                    networkInterface.isUp &&
                        !networkInterface.isLoopback &&
                        networkInterface.supportsMulticast() &&
                        networkInterface.inetAddresses.toList().any { it is Inet4Address }
                }.getOrDefault(false)
            }
        }.getOrDefault(emptyList())

    internal fun buildQuery(): ByteArray {
        val out = ByteArrayOutputStream()
        // Header: id 0, standard query, one question.
        out.write(byteArrayOf(0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0))
        SERVICE.split('.').forEach { label ->
            val bytes = label.encodeToByteArray()
            out.write(bytes.size)
            out.write(bytes)
        }
        out.write(0)
        out.write(byteArrayOf(0, TYPE_PTR.toByte()))
        // Class IN with the unicast-response ("QU") bit set.
        out.write(byteArrayOf(0x80.toByte(), 1))
        return out.toByteArray()
    }

    internal fun parseResponse(bytes: ByteArray, sourceAddress: String?): List<CastDevice> {
        val reader = DnsReader(bytes)
        reader.skip(4)
        val questions = reader.u16()
        val records = reader.u16() + reader.u16() + reader.u16()
        repeat(questions) {
            reader.readName()
            reader.skip(4)
        }
        val instances = linkedSetOf<String>()
        val services = HashMap<String, Pair<String, Int>>()
        val txt = HashMap<String, Map<String, String>>()
        val addresses = HashMap<String, String>()
        repeat(records) {
            val name = reader.readName()
            val type = reader.u16()
            reader.skip(2 + 4)
            val length = reader.u16()
            val end = reader.position + length
            when (type) {
                TYPE_PTR -> if (name.equals(SERVICE, ignoreCase = true)) instances += reader.readName()
                TYPE_SRV -> {
                    reader.skip(4)
                    val port = reader.u16()
                    services[name.lowercase()] = reader.readName() to port
                }
                TYPE_TXT -> txt[name.lowercase()] = reader.readTxt(end)
                TYPE_A -> if (length == 4) {
                    addresses[name.lowercase()] = (0 until 4).joinToString(".") { reader.u8().toString() }
                }
            }
            reader.position = end
        }
        // Some responders answer the SRV/TXT directly without repeating the PTR.
        services.keys.forEach { key ->
            if (key.endsWith(".$SERVICE".lowercase()) && instances.none { it.equals(key, ignoreCase = true) }) {
                instances += key
            }
        }
        return instances.mapNotNull { instance ->
            val key = instance.lowercase()
            val attributes = txt[key].orEmpty()
            val (target, port) = services[key] ?: (null to CastChannel.DEFAULT_PORT)
            val host = target?.let { addresses[it.lowercase()] } ?: sourceAddress ?: return@mapNotNull null
            // "ca" is a capability bitmask; bit 0 is video output. Speakers and speaker groups
            // cannot show video, so they are not offered as cast targets.
            val capabilities = attributes["ca"]?.toIntOrNull()
            if (capabilities != null && capabilities and 1 == 0) return@mapNotNull null
            val fallbackName = instance.substringBefore("._googlecast").substringBeforeLast('-')
            CastDevice(
                id = attributes["id"]?.takeIf { it.isNotBlank() } ?: key,
                name = attributes["fn"]?.takeIf { it.isNotBlank() } ?: fallbackName,
                model = attributes["md"].orEmpty(),
                host = host,
                port = port,
            )
        }
    }

    private const val TYPE_A = 1
    private const val TYPE_PTR = 12
    private const val TYPE_TXT = 16
    private const val TYPE_SRV = 33
}

private class DnsReader(private val bytes: ByteArray) {
    var position = 0

    fun u8(): Int {
        require(position < bytes.size) { "Truncated DNS message" }
        return bytes[position++].toInt() and 0xFF
    }

    fun u16(): Int = (u8() shl 8) or u8()

    fun skip(count: Int) {
        require(position + count <= bytes.size) { "Truncated DNS message" }
        position += count
    }

    fun readName(): String {
        val labels = mutableListOf<String>()
        var cursor = position
        var jumped = false
        var jumps = 0
        while (true) {
            require(cursor < bytes.size) { "Truncated DNS name" }
            val length = bytes[cursor].toInt() and 0xFF
            when {
                length == 0 -> {
                    cursor += 1
                    break
                }
                length and 0xC0 == 0xC0 -> {
                    require(cursor + 1 < bytes.size) { "Truncated DNS pointer" }
                    require(++jumps < 32) { "DNS pointer loop" }
                    val pointer = ((length and 0x3F) shl 8) or (bytes[cursor + 1].toInt() and 0xFF)
                    if (!jumped) position = cursor + 2
                    jumped = true
                    cursor = pointer
                }
                else -> {
                    require(cursor + 1 + length <= bytes.size) { "Truncated DNS label" }
                    labels += bytes.decodeToString(cursor + 1, cursor + 1 + length)
                    cursor += 1 + length
                }
            }
        }
        if (!jumped) position = cursor
        return labels.joinToString(".")
    }

    fun readTxt(end: Int): Map<String, String> {
        val values = LinkedHashMap<String, String>()
        while (position < end) {
            val length = u8()
            require(position + length <= end) { "Truncated TXT record" }
            val entry = bytes.decodeToString(position, position + length)
            position += length
            val separator = entry.indexOf('=')
            if (separator > 0) values[entry.substring(0, separator).lowercase()] = entry.substring(separator + 1)
        }
        return values
    }
}
