package io.polyglotted.eswrapper.services;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;

import java.util.ArrayList;
import java.util.List;

import static io.polyglotted.eswrapper.indexing.IndexSerializer.GSON;

@EqualsAndHashCode(doNotUseGetters = true)
@RequiredArgsConstructor
public class Trade {
    public static final String TRADE_TYPE = "Trade";

    public static final String FieldRegion = "region";
    public static final String FieldDate = "tradeDate";
    public static final String FieldValue = "value";

    public final String address;
    public final String region;
    public final String country;
    public final String city;
    public final String exchange;
    public final String trader;
    public final long tradeDate;
    public final double value;

    public static Trade trade(String address, String region, String country, String
       city, String exchange, String trader, long date, double value) {
        return new Trade(address, region, country, city, exchange, trader, date, value);
    }

    public static BulkRequest tradesRequest(String index, long version) {
        List<Trade> trades = sampleTrades();
        return tradesRequest(index, version, trades);
    }

    public static List<Trade> sampleTrades() {
        List<Trade> trades = new ArrayList<>(25);
        trades.add(trade("/trades/001", "EMEA", "UK", "London", "IEU", "Alex", 1425427200000L, 20.0));
        trades.add(trade("/trades/002", "EMEA", "UK", "London", "IEU", "Andrew", 1420848000000L, 15.0));
        trades.add(trade("/trades/003", "EMEA", "UK", "London", "IEU", "Bob", 1425427200000L, 12.0));
        trades.add(trade("/trades/004", "EMEA", "UK", "London", "NYM", "Charlie", 1423958400000L, 25.0));
        trades.add(trade("/trades/005", "EMEA", "UK", "London", "LME", "Chandler", 1422144000000L, 20.0));
        trades.add(trade("/trades/006", "EMEA", "UK", "London", "LME", "Duncan", 1420848000000L, 10.0));
        trades.add(trade("/trades/007", "EMEA", "UK", "London", "LME", "David", 1423958400000L, 30.0));
        trades.add(trade("/trades/008", "EMEA", "CH", "Geneva", "IEU", "Elliott", 1422144000000L, 20.0));
        trades.add(trade("/trades/009", "EMEA", "CH", "Geneva", "NYM", "Fred", 1425427200000L, 16.0));
        trades.add(trade("/trades/010", "EMEA", "CH", "Zurich", "NYM", "Gabriel", 1423958400000L, 32.0));
        trades.add(trade("/trades/011", "EMEA", "CH", "Zurich", "IUS", "Pier", 1422144000000L, 11.0));
        trades.add(trade("/trades/012", "NA", "US", "Stamford", "IUS", "Richard", 1420848000000L, 20.0));
        trades.add(trade("/trades/013", "NA", "US", "Stamford", "IUS", "Rose", 1425427200000L, 50.0));
        trades.add(trade("/trades/014", "NA", "US", "Stamford", "NYM", "Stan", 1423958400000L, 10.0));
        trades.add(trade("/trades/015", "NA", "US", "Houston", "IUS", "Shaun", 1425427200000L, 50.0));
        trades.add(trade("/trades/016", "NA", "US", "Houston", "NYM", "Alex", 1425427200000L, 40.0));
        trades.add(trade("/trades/017", "SA", "Brazil", "Sao Paulo", "NYM", "Charlie", 1420848000000L, 22.0));
        trades.add(trade("/trades/018", "SA", "Brazil", "Sao Paulo", "NYM", "David", 1422144000000L, 18.0));
        trades.add(trade("/trades/019", "SA", "Brazil", "Sao Paulo", "NYM", "Fred", 1423958400000L, 10.0));
        trades.add(trade("/trades/020", "APAC", "HK", "Hong Kong", "IEU", "John", 1420848000000L, 16.0));
        return trades;
    }

    public static BulkRequest tradesRequest(String index, long version, List<Trade> trades) {
        BulkRequest bulkRequest = new BulkRequest().refresh(true);
        for (Trade trade : trades) {
            bulkRequest.add(new IndexRequest(index, TRADE_TYPE, trade.address)
               .opType(IndexRequest.OpType.CREATE).version(version).versionType(VersionType.EXTERNAL)
               .source(GSON.toJson(trade)));
        }
        return bulkRequest;
    }
}
