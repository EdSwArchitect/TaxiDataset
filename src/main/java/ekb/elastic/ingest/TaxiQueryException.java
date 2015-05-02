package ekb.elastic.ingest;

/**
 * Created by ekbrown on 5/1/15.
 */
public class TaxiQueryException extends Exception {
    /**
     *
     */
    public TaxiQueryException() {
        super();
    }

    /**
     *
     * @param message
     */
    public TaxiQueryException(String message) {
        super(message);
    }

    /**
     *
     * @param message
     * @param cause
     */
    public TaxiQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     *
     * @param cause
     */
    public TaxiQueryException(Throwable cause) {
        super(cause);
    }

    /**
     *
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    protected TaxiQueryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
