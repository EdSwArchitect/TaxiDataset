package ekb.elastic.ingest;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

/**
 * Created by ekbrown on 5/1/15.
 */
public class TaxiQuery {
    private static Logger log = LoggerFactory.getLogger(TaxiQuery.class);
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    public TaxiQuery() {

    }

    /**
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

                rez = new TaxiStats((long) upmin.getValue(),
                        (long) upmax.getValue(), (long) offmin.getValue(), (long) offmax.getValue());

            } // if (resp.status().getStatus() == 200) {
        }
        catch (InterruptedException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());

            throw new TaxiQueryException("Failure in query to elasticsearch", e);
        }
        catch (ExecutionException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());

            throw new TaxiQueryException("Failure in query to elasticsearch", e);
        }

        return rez;
    }

    /**
     * @param client
     * @param index
     * @param startDate
     * @param endDate
     * @param interval
     * @return
     * @throws TaxiQueryException
     */
    public ArrayList<TaxiBucket> getInterval(Client client, String[] index,
                                             String startDate, String endDate, int interval) throws TaxiQueryException {
        ArrayList<TaxiBucket> list = new ArrayList<TaxiBucket>();
        try {

            RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("pickup_datetime").
                    from(sdf.parse(startDate)).to(sdf.parse(endDate));

            SearchRequestBuilder srb = client.prepareSearch(index)
                    .setQuery(rangeQuery).setSearchType(SearchType.COUNT).
                            addAggregation(AggregationBuilders.dateHistogram("histogram").
                                    subAggregation(AggregationBuilders.terms("cabbies").field("hack_license")).
                                    interval(DateHistogram.Interval.minutes(interval)).
                                    field("pickup_datetime"));

            SearchResponse resp = null;
            resp = srb.execute().get();

            log.info("Rest status: " + resp.status().getStatus());

            if (resp.status().getStatus() == 200) {
                log.info("Total hits in range: " + resp.getHits().totalHits());

                DateHistogram agg = resp.getAggregations().get("histogram");

                log.info("list size: " + agg.getBuckets().size());

                for (DateHistogram.Bucket entry : agg.getBuckets()) {


                    String key = entry.getKey();

                    long count = entry.getDocCount();
                    log.info("\tDate: " + key + " Count: " + count);

                    Terms terms = entry.getAggregations().get("cabbies");

                    for (Terms.Bucket tentry : terms.getBuckets()) {
                        log.info("\t\t Term key: " + tentry.getKey());
                        log.info("\t\t Term key count: " + tentry.getDocCount());

                        TaxiBucket tb = new TaxiBucket();
                        tb.setIntervalTime(interval);
                        tb.setMedallion(tentry.getKey());
                        tb.setIntervalTime(entry.getKeyAsDate().getMillis());

                        list.add(tb);
                    }

                } // for (DateHistogram.Bucket entry : agg.getBuckets()) {
            } // if (resp.status().getStatus() == 200) {
        }
        catch (InterruptedException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());

            throw new TaxiQueryException("Failure in query to elasticsearch", e);
        }
        catch (ExecutionException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());

            throw new TaxiQueryException("Failure in query to elasticsearch", e);
        }
        catch (java.text.ParseException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());

            throw new TaxiQueryException("Failure in date format", e);
        }

        return list;
    }

    /**
     *
     * @param client
     * @param index
     * @param medallion
     * @return
     * @throws TaxiQueryException
     */
    public long getFareCount(Client client, String[] index, String medallion) throws TaxiQueryException {
        long count = 0;

        try {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

            TermQueryBuilder medallianQuery = QueryBuilders.termQuery("medallion", medallion);

            boolQuery = boolQuery.must(medallianQuery);

            MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("medallion", medallion);

            SearchRequestBuilder srb = client.prepareSearch(index)
                    .setQuery(matchQuery).setSearchType(SearchType.COUNT);

            log.warn(">>>>>> QUERY TEXT: " + srb.toString());

            SearchResponse resp = null;
            resp = srb.execute().get();

            log.info("Rest status: " + resp.status().getStatus());

            if (resp.status().getStatus() == 200) {
                log.info("Total hits in range: " + resp.getHits().totalHits());

                count = resp.getHits().totalHits();

            } // if (resp.status().getStatus() == 200) {
        }
        catch (InterruptedException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());

            throw new TaxiQueryException("Failure in query to elasticsearch", e);
        }
        catch (ExecutionException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());

            throw new TaxiQueryException("Failure in query to elasticsearch", e);
        }

        return count;
    }

    public long getFareCount(Client client, String[] index, String medallion,
                                             String startDate, String endDate) throws TaxiQueryException {
        long count = 0;

        try {

            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

            RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("pickup_datetime").
                    from(sdf.parse(startDate)).to(sdf.parse(endDate));

            boolQuery = boolQuery.must(rangeQuery);

            MatchQueryBuilder medallianQuery = QueryBuilders.matchQuery("medallion", medallion);

            boolQuery = boolQuery.must(medallianQuery);

            SearchRequestBuilder srb = client.prepareSearch(index)
                    .setQuery(boolQuery).setSearchType(SearchType.COUNT);

            SearchResponse resp = null;
            resp = srb.execute().get();

            log.info("Rest status: " + resp.status().getStatus());

            if (resp.status().getStatus() == 200) {
                log.info("Total hits in range: " + resp.getHits().totalHits());

                count = resp.getHits().totalHits();

            } // if (resp.status().getStatus() == 200) {
        }
        catch (InterruptedException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());

            throw new TaxiQueryException("Failure in query to elasticsearch", e);
        }
        catch (ExecutionException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());

            throw new TaxiQueryException("Failure in query to elasticsearch", e);
        }
        catch (java.text.ParseException e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.error(sw.toString());

            throw new TaxiQueryException("Failure in date format", e);
        }

        return count;
    }

//
//    public static void main(String... args) {
//
//        Options options = new Options();
//        HelpFormatter help = new HelpFormatter();
//
//        try {
//            Option hostOpt = new Option("h", "host", true, "ElasticSearch URL");
//            hostOpt.setArgs(1);
//            hostOpt.setRequired(true);
//            Option portOpt = new Option("p", "port", true, "ElasticSearch URL");
//            portOpt.setArgs(1);
//            portOpt.setRequired(true);
//            Option clusterOpt = new Option("c", "cluster", true, "Cluster");
//            clusterOpt.setArgs(1);
//            clusterOpt.setRequired(true);
//            Option indexOpt = new Option("i", "index", true, "The index");
//            indexOpt.setArgs(1);
//            indexOpt.setRequired(true);
//
//            Option pickupTimeOpt = new Option("u", "pickup", true, "The pickup time");
//            pickupTimeOpt.setArgs(1);
//            pickupTimeOpt.setRequired(true);
//            Option dropTimeOpt = new Option("d", "dropoff", true, "The dropoff time");
//            dropTimeOpt.setArgs(1);
//            dropTimeOpt.setRequired(true);
//
//            options.addOption(hostOpt);
//            options.addOption(portOpt);
//            options.addOption(clusterOpt);
//            options.addOption(indexOpt);
//            options.addOption(pickupTimeOpt);
//            options.addOption(dropTimeOpt);
//
//            GnuParser parser = new GnuParser();
//            CommandLine cmd = parser.parse(options, args);
//
//            Settings settings = ImmutableSettings.settingsBuilder()
//                    .put("cluster.name", cmd.getOptionValue('c')).build();
//            Client client = new TransportClient(settings).
//                    addTransportAddress(new InetSocketTransportAddress(cmd.getOptionValue('h'),
//                            Integer.parseInt(cmd.getOptionValue('p'))));
//
//            TaxiQuery tq = new TaxiQuery();
//
//            TaxiStats stats = tq.getTaxiStats(client, cmd.getOptionValues("i"));
//
//            log.info("Results:\n" + stats.toDateString());
//
//            sdf.parse(cmd.getOptionValue("u"));
//            sdf.parse(cmd.getOptionValue("d"));
//
//            // 2013-01-01T10:10:00
//
//            ArrayList<TaxiBucket> list = tq.getInterval(client, cmd.getOptionValues("index"),
//                    cmd.getOptionValue("u"), cmd.getOptionValue("d"), 60);
//
//            log.info("List size is: " + list.size());
//
//            client.close();
//
//        }
//        catch (ParseException pe) {
//            StringWriter sw = new StringWriter();
//            PrintWriter pw = new PrintWriter(sw);
//
//            help.printUsage(pw, 80, TaxiQuery.class.getName(), options);
//
//            log.error(sw.toString());
//
//        }
//        catch (TaxiQueryException e) {
//            StringWriter sw = new StringWriter();
//            PrintWriter pw = new PrintWriter(sw);
//            e.printStackTrace(pw);
//            log.error(sw.toString());
//        }
//        catch (java.text.ParseException e) {
//            StringWriter sw = new StringWriter();
//            PrintWriter pw = new PrintWriter(sw);
//            e.printStackTrace(pw);
//            log.error(sw.toString());
//        }
//    }
}
