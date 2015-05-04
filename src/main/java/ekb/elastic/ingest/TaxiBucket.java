package ekb.elastic.ingest;

/**
 * Created by ekbrown on 5/4/15.
 */
public class TaxiBucket {
    private String medallion;
    private long pickupInterval;
    private long intervalTime;

    /**
     *
     */
    public TaxiBucket() {

    }

    /**
     *
     * @param medallion
     * @param pickupInterval
     * @param intervalTime
     */
    public TaxiBucket(String medallion, long pickupInterval, long intervalTime) {
        this.medallion = medallion;
        this.pickupInterval = pickupInterval;
        this.intervalTime = intervalTime;
    }

    /**
     *
     * @return
     */
    public String getMedallion() {
        return medallion;
    }

    /***
     *
     * @param medallion
     */
    public void setMedallion(String medallion) {
        this.medallion = medallion;
    }

    /**
     *
     * @return
     */
    public long getPickupInterval() {
        return pickupInterval;
    }

    /**
     *
     * @param pickupInterval
     */
    public void setPickupInterval(long pickupInterval) {
        this.pickupInterval = pickupInterval;
    }

    /**
     *
     * @return
     */
    public long getIntervalTime() {
        return intervalTime;
    }

    /**
     *
     * @param intervalTime
     */
    public void setIntervalTime(long intervalTime) {
        this.intervalTime = intervalTime;
    }

    /**
     *
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TaxiBucket that = (TaxiBucket) o;

        if (pickupInterval != that.pickupInterval) {
            return false;
        }
        if (intervalTime != that.intervalTime) {
            return false;
        }
        return !(medallion != null ? !medallion.equals(that.medallion) : that.medallion != null);

    }

    /**
     *
     * @return
     */
    @Override
    public int hashCode() {
        int result = medallion != null ? medallion.hashCode() : 0;
        result = 31 * result + (int) (pickupInterval ^ (pickupInterval >>> 32));
        result = 31 * result + (int) (intervalTime ^ (intervalTime >>> 32));
        return result;
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return "TaxiBucket{" +
                "medallion='" + medallion + '\'' +
                ", pickupInterval=" + pickupInterval +
                ", intervalTime=" + intervalTime +
                '}';
    }
}
