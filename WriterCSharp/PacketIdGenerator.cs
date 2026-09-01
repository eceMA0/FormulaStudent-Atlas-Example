namespace WriterCSharp;

internal class PacketIdGenerator
{
    private ulong packetId;

    public ulong GetPacketId()
    {
        var packetId = this.packetId;
        this.packetId++;
        return packetId;
    }
}
