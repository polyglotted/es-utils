package applauncher.support;

import java.net.InetAddress;

public final class NodeIdentifier {
    public final String identifier;

    public NodeIdentifier() throws Exception {
        this("Unknown");
    }

    public NodeIdentifier(String serverName)throws Exception  {
        this.identifier = serverName + "::(" + localHost() + ")";
    }

    static String localHost() throws Exception {
        return InetAddress.getLocalHost().getHostName();
    }
}
