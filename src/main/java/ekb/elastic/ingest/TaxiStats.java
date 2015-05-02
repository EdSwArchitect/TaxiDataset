package ekb.elastic.ingest;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created by ekbrown on 5/1/15.
 */
public class TaxiStats {
    public long firstPickup;
    public long lastPickup;
    public long firstDropoff;
    public long lastDropoff;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    /**
     *
     */
    public TaxiStats() {

    }

    /**
     *
     * @param firstPickup
     * @param lastPickup
     * @param firstDropoff
     * @param lastDropoff
     */
    public TaxiStats(long firstPickup, long lastPickup, long firstDropoff, long lastDropoff) {
        this.firstPickup = firstPickup;
        this.lastPickup = lastPickup;
        this.firstDropoff = firstDropoff;
        this.lastDropoff = lastDropoff;
    }

    /**
     *
     * @return
     */
    public long getFirstPickup() {
        return firstPickup;
    }

    /**
     *
     * @param firstPickup
     */
    public void setFirstPickup(long firstPickup) {
        this.firstPickup = firstPickup;
    }

    /**
     *
     * @return
     */
    public long getLastPickup() {
        return lastPickup;
    }

    /**
     *
     * @param lastPickup
     */
    public void setLastPickup(long lastPickup) {
        this.lastPickup = lastPickup;
    }

    /**
     *
     * @return
     */
    public long getFirstDropoff() {
        return firstDropoff;
    }

    /**
     *
     * @param firstDropoff
     */
    public void setFirstDropoff(long firstDropoff) {
        this.firstDropoff = firstDropoff;
    }

    /**
     *
     * @return
     */
    public long getLastDropoff() {
        return lastDropoff;
    }

    /**
     *
     * @param lastDropoff
     */
    public void setLastDropoff(long lastDropoff) {
        this.lastDropoff = lastDropoff;
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

        TaxiStats taxiStats = (TaxiStats) o;

        if (firstPickup != taxiStats.firstPickup) {
            return false;
        }
        if (lastPickup != taxiStats.lastPickup) {
            return false;
        }
        if (firstDropoff != taxiStats.firstDropoff) {
            return false;
        }
        return lastDropoff == taxiStats.lastDropoff;

    }

    /**
     *
     * @return
     */
    @Override
    public int hashCode() {
        int result = (int) (firstPickup ^ (firstPickup >>> 32));
        result = 31 * result + (int) (lastPickup ^ (lastPickup >>> 32));
        result = 31 * result + (int) (firstDropoff ^ (firstDropoff >>> 32));
        result = 31 * result + (int) (lastDropoff ^ (lastDropoff >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "TaxiStats{" +
                "firstPickup=" + firstPickup +
                ", lastPickup=" + lastPickup +
                ", firstDropoff=" + firstDropoff +
                ", lastDropoff=" + lastDropoff +
                '}';
    }

    public String toDateString() {
        return "TaxiStats{" +
                "firstPickup=" + sdf.format(firstPickup) +
                ", lastPickup=" + sdf.format(lastPickup) +
                ", firstDropoff=" + sdf.format(firstDropoff) +
                ", lastDropoff=" + sdf.format(lastDropoff) +
                '}';
    }
}
