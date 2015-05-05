package ekb.elastic.services;

import java.util.Date;

/**
 * Created by ekbrown on 5/4/15.
 */
public interface TaxiServices {
    /**
     * Did the taxi have a fare
     * @param medallian
     * @param from
     * @param to
     * @return
     */
    public boolean taxiHadFares(String medallian, Date from, Date to);

    /**
     * Number of fares for the taxi
     * @param medallian
     * @param from
     * @param to
     * @return
     */
    public int getTaxiFareCount(String medallian, Date from, Date to);
}
