package it.polimi.ds.chat.client.connection;

import java.io.ObjectInputStream;
import java.net.Socket;

/** Test-only bridge for constructing a scripted connection generation. */
public final class ClientGenerationTestFactory {

    private ClientGenerationTestFactory() {
    }

    public static ClientConnectionGeneration create(
            long id,
            String host,
            int port,
            Socket socket,
            ObjectInputStream input,
            ClientObjectWriter writer
    ) {
        return new ClientConnectionGeneration(
                id,
                host,
                port,
                socket,
                input,
                writer
        );
    }
}
