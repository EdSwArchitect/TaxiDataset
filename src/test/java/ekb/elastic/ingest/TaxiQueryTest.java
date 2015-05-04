package ekb.elastic.ingest;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Created by ekbrown on 5/4/15.
 */
public class TaxiQueryTest {
    private Client client;
    private static Logger log = LoggerFactory.getLogger(TaxiQueryTest.class);

    @Before
    public void setUp() throws Exception {
        log.info("Setup called");

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "edwin").build();
        client = new TransportClient(settings).
                addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

    }

    @After
    public void tearDown() throws Exception {
        log.info("Setup called");
        client.close();
    }

    @Test
    public void testGetTaxiStats() throws Exception {
        TaxiQuery tq = new TaxiQuery();
        String[] indices = { "taxi-geo" };

        TaxiStats stats = tq.getTaxiStats(client, indices);

        log.info("The stats are: " + stats);

        Assert.assertNotNull("Stats shouldn't be null", stats);

        Assert.assertEquals("firstPickup wrong", 1357016400000L, stats.getFirstPickup());
        Assert.assertEquals("lastPickup wrong", 1370059199000L, stats.getLastPickup());
        Assert.assertEquals("firstDropoff wrong", 1357016436000L, stats.getFirstDropoff());
        Assert.assertEquals("lastDropoff wrong", 1370068607000L, stats.getLastDropoff());
    }

    @Test
    public void testGetInterval() throws Exception {
        String[] indices = { "taxi-geo" };
        TaxiQuery tq = new TaxiQuery();

        //  "2013-01-03T10:10:00"  --dropoff "2013-01-03T23:10:00"

        ArrayList<TaxiBucket> list = tq.getInterval(client, indices,
                "2013-01-05T3:10:00", "2013-01-05T23:10:00", 60);

        log.info("Size of list is: " + list.size());

        Assert.assertTrue("This list size greater than zero", true);
        Assert.assertEquals("Size is know and you didn't get it", 210, list.size());

    }
}