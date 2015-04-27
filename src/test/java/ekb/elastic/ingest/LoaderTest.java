package ekb.elastic.ingest;

import junit.framework.TestCase;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
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

        IndicesExistsRequest ier = new IndicesExistsRequest("test_index");

        IndicesExistsResponse resp = client.admin().indices().exists(ier).actionGet();

        if (resp.isExists()) {

            DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest("test_index")).actionGet();

            log.info("Delete of test-index isAcknowledged? " + delete.isAcknowledged());
        }

        ier = new IndicesExistsRequest("anotherindex");
        resp = client.admin().indices().exists(ier).actionGet();

        if (resp.isExists()) {

            DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest("anotherindex")).actionGet();

            log.info("Delete of test-index2 isAcknowledged? " + delete.isAcknowledged());
        }

        client.close();
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testParseFile() throws Exception {
        log.info("testParseFile");
//        Loader loader = new Loader("src/test/resources/test_trip_data.csv", 100);

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

        boolean created = loader.createIndex(client, "test_index", "testme");

        log.info("Test-index created? " + created);
        Assert.assertTrue("Expected the index 'test-index' to be created", created);

        boolean exists = loader.indexExists(client, "test_index");

        Assert.assertTrue("Expected the index 'test-index' to exist", exists);

        client.close();
    }

    /**
     *
     * @throws Exception
     */
//    @Test
    public void testIndexExists() throws Exception {
        log.info("testIndexExists");

        Loader loader = new Loader();
        Client client = loader.connectES();

        boolean created = loader.createIndex(client, "anotherindex", null);

        log.info("anotherindex created? " + created);

        boolean exists = loader.indexExists(client, "anotherindex");

        log.info("anotherindex exists? " + exists);

        client.close();

//        loader.closeLoader();

//        Assert.assertTrue("Expected the index 'anotherindex' to exist", exists);
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