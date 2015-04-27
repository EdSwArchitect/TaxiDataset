package ekb.elastic.ingest;

import junit.framework.TestCase;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ekbrown on 4/26/15.
 */
public class LoaderTest extends TestCase {
    private Client client;
    private Logger log = LoggerFactory.getLogger(LoaderTest.class);

    /**
     *
     * @throws Exception
     */
    public void setUp() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "edwin").build();
        client = new TransportClient(settings).
                addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
    }

    /**
     *
     * @throws Exception
     */
    public void tearDown() throws Exception {

        DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest("test-index")).actionGet();

        log.info("Delete of test-index isAcknowledged? " + delete.isAcknowledged());

        delete = client.admin().indices().delete(new DeleteIndexRequest("test-index2")).actionGet();

        Thread.sleep(1000*5);

        log.info("Delete of test-index2 isAcknowledged? " + delete.isAcknowledged());

        client.close();
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testParseFile() throws Exception {
        log.info("testParseFile");
        Loader loader = new Loader("src/test/resources/test_trip_data.csv", 100);

//        loader.closeLoader();
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testCreateIndex() throws Exception {
        Loader loader = new Loader();
        Client client = loader.connectES();

        boolean created = loader.createIndex(client, "test-index", "test", "testme");

        log.info("Test-index created? " + created);

        Thread.sleep(1000 * 5);

//        loader.closeLoader();

        client.close();
        Assert.assertTrue("Expected the index 'test-index' to be created", created);
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testIndexExists() throws Exception {
        log.info("testIndexExists");

        Loader loader = new Loader();
        Client client = loader.connectES();

        boolean created = loader.createIndex(client, "test-index2", "test", "testme2");

        log.info("test-index2 created? " + created);

        Thread.sleep(1000 * 5);

        boolean exists = loader.indexExists(client, "test-index2");

        log.info("test-index2 exists? " + exists);

//        loader.closeLoader();

        Assert.assertTrue("Expected the index 'test-index2' to exist", exists);
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testBulkLoad() throws Exception {

        Assert.assertTrue("This should always be true", true);
        log.info("testBulkLoad");

        Loader loader = new Loader("src/test/resources/test_trip_data.csv", 100);

//        loader.closeLoader();

    }
}