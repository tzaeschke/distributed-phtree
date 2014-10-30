package ch.ethz.globis.distindex.client.exception;

/**
 * Thrown when the response received from the server does not have the proper metadata for a response to
 * the request sent. This happens if the requestId on the response does not match the requestId of the sent request.
 *
 */
public class InvalidResponseException extends IllegalStateException {

    public InvalidResponseException() {
    }

    public InvalidResponseException(String s) {
        super(s);
    }

    public InvalidResponseException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidResponseException(Throwable cause) {
        super(cause);
    }
}
