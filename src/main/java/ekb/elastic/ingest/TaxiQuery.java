package ekb.elastic.ingest;

import org.apache.commons.cli.*;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ExecutionException;

/**
 * Created by ekbrown on 5/1/15.
 */
public class TaxiQuery {
    private static Logger log = LoggerFactory.getLogger(TaxiQuery.class);

    public TaxiQuery() {

    }

    /**
     *
     * @param client
     * @param index
     * @return
     * @throws TaxiQueryException
     */
    public TaxiStats getTaxiStats(Client client, String... index) throws TaxiQueryException {

        TaxiStats rez = null;
        try {
        SearchRequestBuilder srb = client.prepareSearch(index)
                .setQuery(QueryBuilders.matchAllQuery()).
                        addAggregation(AggregationBuilders.max("pickup_max").field("pickup_datetime")).
                        addAggregation(AggregationBuilders.min("pickup_min").field("pickup_datetime")).
                        addAggregation(AggregationBuilders.max("dropoff_max").field("dropoff_datetime")).
                        addAggregation(AggregationBuilders.min("dropoff_min").field("dropoff_datetime"));

        SearchResponse resp = null;
            resp = srb.execute().get();

        log.info("Rest status: " + resp.status().getStatus());

        if (resp.status().getStatus() == 200) {
            Max upmax = resp.getAggregations().get("pickup_max");
            Max offmax = resp.getAggregations().get("dropoff_max");
            Min upmin = resp.getAggregations().get("pickup_min");
            Min offmin = resp.getAggregations().get("dropoff_min");

            rez = new TaxiStats((long)upmin.getValue(),
                    (long)upmax.getValue(), (long)offmin.getValue(), (long)offmax.getValue());

        } // if (resp.status().getStatus() == 200) {
        }
        catch (InterruptedException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());

            throw new TaxiQueryException("Failure in query to elasticsearch" , e);
        }
        catch (ExecutionException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());

            throw new TaxiQueryException("Failure in query to elasticsearch" , e);
        }

        return rez;
    }

    public static void main(String ... args) {

        Options options = new Options();
        HelpFormatter help = new HelpFormatter();

        try {
            Option hostOpt = new Option("h", "host", true, "ElasticSearch URL");
            hostOpt.setArgs(1);
            hostOpt.setRequired(true);
            Option portOpt = new Option("p", "port", true, "ElasticSearch URL");
            portOpt.setArgs(1);
            portOpt.setRequired(true);
            Option clusterOpt = new Option("c", "cluster", true, "Cluster");
            clusterOpt.setArgs(1);
            clusterOpt.setRequired(true);
            Option indexOpt = new Option("i", "index", true, "The index");
            indexOpt.setArgs(1);
            indexOpt.setRequired(true);

            options.addOption(hostOpt);
            options.addOption(portOpt);
            options.addOption(clusterOpt);
            options.addOption(indexOpt);

            GnuParser parser = new GnuParser();
            CommandLine cmd = parser.parse(options, args);

            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("cluster.name", cmd.getOptionValue('c')).build();
            Client client = new TransportClient(settings).
                    addTransportAddress(new InetSocketTransportAddress(cmd.getOptionValue('h'),
                            Integer.parseInt(cmd.getOptionValue('p'))));

            TaxiQuery tq = new TaxiQuery();

            TaxiStats stats = tq.getTaxiStats(client, cmd.getOptionValues("i"));

            log.info("Results:\n" + stats.toDateString());


//            SearchRequestBuilder srb = client.prepareSearch(cmd.getOptionValues("i"))
//                    .setQuery(QueryBuilders.matchAllQuery()).
//                            addAggregation(AggregationBuilders.max("pickup_max").field("pickup_datetime")).
//                            addAggregation(AggregationBuilders.min("pickup_min").field("pickup_datetime")).
//                            addAggregation(AggregationBuilders.max("dropoff_max").field("dropoff_datetime")).
//                            addAggregation(AggregationBuilders.min("dropoff_min").field("dropoff_datetime"));
//
//            SearchResponse resp = srb.execute().get();
//
//            log.info("Rest status: " + resp.status().getStatus());
//
//            if (resp.status().getStatus() == 200) {
//                Max upmax = resp.getAggregations().get("pickup_max");
//                Max offmax = resp.getAggregations().get("dropoff_max");
//                Min upmin = resp.getAggregations().get("pickup_min");
//                Min offmin = resp.getAggregations().get("dropoff_min");
//
//                log.info("first pickup: " + new Date((long)upmin.getValue()));
//                log.info("last pickup: " + new Date((long)upmax.getValue()));
//                log.info("first dropoff: " + new Date((long)offmin.getValue()));
//                log.info("last dropoff: " + new Date((long)offmax.getValue()));
//
//            } // if (resp.status().getStatus() == 200) {

            client.close();

        }
        catch (ParseException pe) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);

            help.printUsage(pw, 80, TaxiQuery.class.getName(), options);

            log.error(sw.toString());

        }
        catch (TaxiQueryException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());
        }
    }
}
