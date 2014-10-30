package ch.ethz.globis.distindex.client.exception;

/**
 * Thrown when the remote server is available but the current request
 * could not be serviced due to an error on the server.
 */
public class ServerErrorException extends IllegalStateException {

    public ServerErrorException() {
    }

    public ServerErrorException(String s) {
        super(s);
    }

    public ServerErrorException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServerErrorException(Throwable cause) {
        super(cause);
    }
}
