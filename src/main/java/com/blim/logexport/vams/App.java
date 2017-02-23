package com.blim.logexport.vams;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.FilterLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.reactivex.Flowable;

public class App 
{
    public static void main( String[] args ) throws IOException
    {
        /*
        'VAMS-BI-TEST' 'vamsd-events-content_view' 1485279710000 /tmp/test.csv
        'VAMS-BI-PROD' 'vamsd-events-content_view' 1485279710000 /tmp/prod.csv 
        */
        final String groupName = args[0];
        final String streamName = args[1];
        final long startTime = Long.parseLong(args[2]);
        final String fileName = args[3];

        final FileWriter fileWriter = new FileWriter(fileName);
        final CSVPrinter printer = new CSVPrinter(fileWriter, CSVFormat.DEFAULT);

        printer.printRecord("userId", "contentId", "eventType", "eventTime", "Action", "ratingValue");
        
        System.out.println("starting");

        fromFilter(groupName, streamName, startTime, printer);
        
        System.out.println("done");

        fileWriter.close();
        printer.close();
    }
    
    public static void fromGet(String groupName, String streamName, Long startTime) {
        final AWSLogs logs = AWSLogsClientBuilder.defaultClient();
        final MapperWrapper objectMapper = new MapperWrapper(new ObjectMapper());

        final GetLogEventsRequest req = new GetLogEventsRequest()
            .withLogGroupName(groupName)
            .withLogStreamName(streamName)
            .withStartFromHead(true)
            .withStartTime(startTime)
            .withLimit(100);

        Flowable.just(req)
            .repeat()
            .map(logs::getLogEvents)
            .doOnNext(x -> req.setNextToken(x.getNextForwardToken()))
            .doOnNext(x -> System.out.println(x.getEvents().size()))
            .doOnNext(x -> System.out.println(x.getNextBackwardToken()))
            .doOnNext(x -> System.out.println(x.getNextForwardToken()))
            .takeUntil(x -> x.getEvents().size() == 0)
            .flatMapIterable(x -> x.getEvents())
            .map(x -> x.getMessage())
            .filter(x -> x.contains("ACQUIRED"))
            .map(objectMapper::parse)
            .subscribe(System.out::println);
    }
    
    public static void fromFilter(final String groupName, final String streamName, final Long startTime, final CSVPrinter printer) {
        final AWSLogs logs = AWSLogsClientBuilder.defaultClient();
        final MapperWrapper objectMapper = new MapperWrapper(new ObjectMapper());
        
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss.S");

        final FilterLogEventsRequest req = new FilterLogEventsRequest()
            .withLogGroupName(groupName)
            .withLogStreamNames(streamName)
            .withStartTime(startTime)
            .withFilterPattern("ACQUIRED");

        Flowable.just(req)
            .repeat()
            .map(logs::filterLogEvents)
            .doOnNext(x -> req.setNextToken(x.getNextToken()))
            .doOnNext(x -> System.out.println(x.getEvents().size()))
            .doOnNext(x -> System.out.println(x.getNextToken()))
            .takeUntil(x -> x.getNextToken() == null)
            .flatMapIterable(x -> x.getEvents())
            .map(x -> x.getMessage())
            .map(objectMapper::parse)
            .filter(x -> x.data.assetId != null)
            .subscribe(x -> printer.printRecord(x.data.userId, x.data.assetId, "Watch", dateFormat.parse(x.data.creationDate).getTime() / 1000, "", ""));
    }

    /*
     {
          "data": {
            "id": "574845-12c62ce2-33c2-4350-b0af-f17e015c3196",
            "creationDate": "2017-02-23 17:04:00.5373",
            "userId": "574845",
            "assetId": "4006",
            "channelId": null,
            "listingId": null,
            "programType": null,
            "viewStarted": null,
            "viewEnded": null,
            "viewDuration": 2369,
            "timelinePosition": 487,
            "deviceInfo": "type/TABLET make/MBX model/MXQ uid/b37295a156409af7 os/Android osVersion/19 blimVersion/2.1.1",
            "state": "RETAINED"
          },
          "destination": "content_view",
          "event": "createdContentView",
          "level": "info",
          "msg": "vams",
          "time": "2017-02-23T17:04:00Z"
    }
     */
    
    public static final class MapperWrapper {
        private ObjectMapper mapper;

        public MapperWrapper(ObjectMapper mapper) {
            this.mapper = mapper;
        }
        
        public LogMessage parse(String data) {
            try {
                return mapper.readValue(data, LogMessage.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public static final class LogMessage {
        public Data data;
        public String destination;
        public String event;
        public String level;
        public String msg;
        public String time;
    }
    
    public static final class Data {
        public String id;
        public String creationDate;
        public String userId;
        public String assetId;
        public String channelId;
        public String listingId;
        public String programType;
        public String viewStarted;
        public String viewEnded;
        public long viewDuration;
        public long timelinePosition;
        public String deviceInfo;
        public String state;
    }
}
