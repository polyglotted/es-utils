package applauncher.support;

import lombok.SneakyThrows;

import java.net.InetAddress;

public final class NodeIdentifier {
    public final String identifier;

    public NodeIdentifier() {
        this("Unknown");
    }

    public NodeIdentifier(String serverName) {
        this.identifier = serverName + "::(" + localHost() + ")";
    }

    @SneakyThrows
    static String localHost() {
        return InetAddress.getLocalHost().getHostName();
    }
}
