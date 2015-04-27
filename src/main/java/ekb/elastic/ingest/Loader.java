package ekb.elastic.ingest;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ekbrown on 4/22/15.
 */
public class Loader {
    private Logger log = LoggerFactory.getLogger(Loader.class);

    private String filePath;
    private int bufferSize = 100;
    private CSVReader reader;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * Default
     */
    public Loader() {

    }

    /**
     *
     * @param filePath
     */
    public Loader(String filePath) throws FileNotFoundException {
        this.filePath = filePath;

        reader = new CSVReader(new FileReader(filePath), CSVParser.DEFAULT_SEPARATOR,
                CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
    }

    /**
     *
     * @param filePath
     * @param bufferSize
     */
    public Loader(String filePath, int bufferSize) throws FileNotFoundException {
        this.filePath = filePath;
        this.bufferSize = bufferSize;

        reader = new CSVReader(new FileReader(filePath), CSVParser.DEFAULT_SEPARATOR,
                CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
    }

    public ArrayList<String[]> parseFile() throws IOException {
        ArrayList<String[]> list = new ArrayList<String[]>();

        String [] taxiData;

        // medallion,
        // hack_license,
        // vendor_id,
        // rate_code,
        // store_and_fwd_flag,
        // pickup_datetime,
        // dropoff_datetime,
        // passenger_count,
        // trip_time_in_secs,
        // trip_distance,
        // pickup_longitude,
        // pickup_latitude,
        // dropoff_longitude,
        // dropoff_latitude


        for (int i= 0; i < bufferSize && (taxiData = reader.readNext()) != null; i++) {

            list.add(taxiData);
        }

        return list;
    }

    /**
     *
     * @throws IOException
     */
    public void endParsing() throws IOException {
        reader.close();
    }

    /**
     *
     * @return
     */
    public Client connectES() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "edwin").build();
        Client client = new TransportClient(settings).
                addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        return client;
    }

    /**
     *
     * @param client
     * @param index
     * @param alias
     * @return
     */
    public boolean createIndex(Client client, String index, String alias) {
        boolean created = false;

        AdminClient admin = client.admin();
        CreateIndexRequest cir = admin.indices().prepareCreate(index).request();

        CreateIndexResponse cresp = admin.indices().create(cir).actionGet();

        log.info("Response for create index '" + index + "': " + cresp.isAcknowledged());

        created = cresp.isAcknowledged();

        if (cresp.isAcknowledged() && alias != null) {
            IndicesAliasesResponse iar = admin.indices().prepareAliases().addAlias(index, alias).get();

            created = iar.isAcknowledged();
            System.out.println("\tAlias created: " + iar.isAcknowledged());
        }

        return created;
    }

    /**
     *
     * @param client
     * @param index
     * @param alias
     * @param mapping
     * @return
     */
    public boolean createIndex(Client client, String index, String alias, String mapping) {
        boolean created = false;

        AdminClient admin = client.admin();

        CreateIndexRequest cir = admin.indices().prepareCreate(index).request();

        CreateIndexResponse cresp = admin.indices().create(cir).actionGet();

        log.info("Response for creating index '" + index + "': " + cresp.isAcknowledged());

        created = cresp.isAcknowledged();

        if (created && alias != null) {
            IndicesAliasesResponse iar = admin.indices().prepareAliases().addAlias(index, alias).get();

            created = iar.isAcknowledged();
            System.out.println("\tAlias created: " + iar.isAcknowledged());
        }

        PutMappingRequestBuilder pmrb = admin.indices().preparePutMapping(index).setSource(mapping).setType("taxi-geo");

        PutMappingResponse pmr = admin.indices().putMapping(pmrb.request()).actionGet();

        if (pmr.isAcknowledged()) {
            log.info("\t\tMapping set");
        }
        else {
            log.info("\t\tMapping not set!");
        }


        return created;
    }

    /**
     *
     * @param client
     * @param index
     * @return
     */
    public boolean indexExists(Client client, String index) {

        IndicesExistsRequest ier = new IndicesExistsRequest(index);

        IndicesExistsResponse resp = client.admin().indices().exists(ier).actionGet();

        log.info("Index exists from index '" + index + "' returns " + resp.isExists());

        return resp.isExists();
    }

    /**
     *
     * @param client
     * @param list
     * @param index
     * @param indexType
     * @return
     * @throws IOException
     */
    public boolean bulkLoad(Client client, List<String[]> list, String index, String indexType) throws IOException, ParseException {
        // medallion,
        // hack_license,
        // vendor_id,
        // rate_code,
        // store_and_fwd_flag,
        // pickup_datetime,
        // dropoff_datetime,
        // passenger_count,
        // trip_time_in_secs,
        // trip_distance,
        // pickup_longitude,
        // pickup_latitude,
        // dropoff_longitude,
        // dropoff_latitude
        BulkRequestBuilder bqr = client.prepareBulk();

        Double upLong;
        Double upLat;
        Double downLong;
        Double downLat;



        for (String[] taxi : list) {
            try {
                upLong = Double.parseDouble(taxi[11]);
                upLat = Double.parseDouble(taxi[11]);
                downLong = Double.parseDouble(taxi[12]);
                downLat = Double.parseDouble(taxi[13]);

                bqr.add(client.prepareIndex(index, indexType).setSource(
                                XContentFactory.jsonBuilder().startObject().field("medallion", taxi[0])
                                        .field("hack_license", taxi[1])
                                        .field("vendor_id", taxi[2])
                                        .field("rate_code", Integer.parseInt(taxi[3]))
                                        .field("store_fwd_flag", taxi[4])
                                        .field("pickup_datetime", sdf.parse(taxi[5]))
                                        .field("dropoff_datetime", sdf.parse(taxi[6]))
                                        .field("passenger_count", Integer.parseInt(taxi[7]))
                                        .field("trip_time_in_seconds", Long.parseLong(taxi[8]))
                                        .field("trip_distance", Double.parseDouble(taxi[9]))
                                        .field("pickup_longitude", upLong)
                                        .field("pickup_latitude", upLat)
                                        .field("dropoff_longitude", downLong)
                                        .field("dropoff_latitude", downLat)
                                        .endObject())
                );

            }
            catch(NumberFormatException nfe) {

            }
        } // for (String[] taxi : list) {

        BulkResponse bresp = bqr.execute().actionGet();

        return !bresp.hasFailures();
    }

    /**
     *
     * @param client
     * @param list
     * @param index
     * @param indexType
     * @return
     * @throws IOException
     */
    public boolean bulkGeoLoad(Client client, List<String[]> list, String index, String indexType) throws IOException, ParseException {
        // medallion,
        // hack_license,
        // vendor_id,
        // rate_code,
        // store_and_fwd_flag,
        // pickup_datetime,
        // dropoff_datetime,
        // passenger_count,
        // trip_time_in_secs,
        // trip_distance,
        // pickup_longitude,
        // pickup_latitude,
        // dropoff_longitude,
        // dropoff_latitude
        BulkRequestBuilder bqr = client.prepareBulk();

        Double upLong;
        Double upLat;
        Double downLong;
        Double downLat;



        for (String[] taxi : list) {
            try {
                upLong = Double.parseDouble(taxi[11]);
                upLat = Double.parseDouble(taxi[11]);
                downLong = Double.parseDouble(taxi[12]);
                downLat = Double.parseDouble(taxi[13]);

                bqr.add(client.prepareIndex(index, indexType).setSource(
                                XContentFactory.jsonBuilder().startObject().field("medallion", taxi[0])
                                        .field("hack_license", taxi[1])
                                        .field("vendor_id", taxi[2])
                                        .field("rate_code", Integer.parseInt(taxi[3]))
                                        .field("store_fwd_flag", taxi[4])
                                        .field("pickup_datetime", sdf.parse(taxi[5]))
                                        .field("dropoff_datetime", sdf.parse(taxi[6]))
                                        .field("passenger_count", Integer.parseInt(taxi[7]))
                                        .field("trip_time_in_seconds", Long.parseLong(taxi[8]))
                                        .field("trip_distance", Double.parseDouble(taxi[9]))
                                        .startObject("pickupLocation")
                                        .startObject("location")
                                        .field("lon", upLong)
                                        .field("lat", upLat)
                                        .endObject()
                                        .endObject()
                                        .startObject("dropoffLocation")
                                        .startObject("location")
                                        .field("lon", downLong)
                                        .field("lat", downLat)
                                        .endObject()
                                        .endObject()
                                        .endObject())
                );

            }
            catch(NumberFormatException nfe) {

            }
        } // for (String[] taxi : list) {

        BulkResponse bresp = bqr.execute().actionGet();

        return !bresp.hasFailures();
    }

    /**
     *
     */
    public void closeLoader() {
        try {
            this.reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String... args) {
        try {
            System.out.println("Hi, Ed");

            Loader loader = new Loader(args[0], 20000);

            Client client = loader.connectES();

//            boolean created = loader.createIndex(client, "taxi-data", "taxi-data", "taxi");
            if (!loader.indexExists(client, "taxi-geo")) {
                FileInputStream fis = new FileInputStream(args[1]);
                FileChannel channel = fis.getChannel();
                MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());

                Charset utf8 = Charset.forName("UTF-8");

                CharBuffer mapping = utf8.decode(mappedByteBuffer);

                boolean created = loader.createIndex(client, "taxi-geo", "taxigeo", mapping.toString());
                channel.close();
            }

            List<String[]>data;

            boolean processing = true;
            boolean indexed;

            long t0 = System.currentTimeMillis();

            while (processing) {
                data = loader.parseFile();

                if (data.size() > 0) {
//                    indexed = loader.bulkLoad(client, data, "taxi-data", "taxi-data");
                    indexed = loader.bulkGeoLoad(client, data, "taxi-geo", "taxi-geo");

                    System.out.println("Indexed data: " + indexed);

                    if (!indexed) {
                        processing = false;
                    }
                }
                else {
                    processing = false;
                }
            }

            long t1 = System.currentTimeMillis();

            System.out.println("Elaspsed time: " + (t1-t0)/1000);

            client.close();

        }catch(Exception exp) {
            exp.printStackTrace();
        }
    }
}
