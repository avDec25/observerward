package com.myntra.observerward.schedules;

import com.myntra.observerward.service.ConsumerLagService;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class ConsumerLagPublisher {

	@Autowired
	ConsumerLagService consumerLagService;

	@Value("${statsd.prefix}")
	String PREFIX;

	@Value("${statsd.hostname}")
	String HOSTNAME;

	@Value("${statsd.port}")
	String PORT;

	@Value("${datafile.groupids}")
	String DATAFILE_GROUPIDS;


//	stats.gauges.prod.airbusconsumer.consumer_lag.
//	$consumer_app-$producer_app-$event-consumer-group

	@SneakyThrows
//	@Scheduled(fixedRate = 10, timeUnit = TimeUnit.SECONDS)
	public void pushConsumerGroupLag() {
		int port = Integer.parseInt(PORT);
		StatsDClient statsDClient = new NonBlockingStatsDClient(PREFIX, HOSTNAME, port);

		try (LineIterator it = FileUtils.lineIterator(new File(DATAFILE_GROUPIDS), "UTF-8")) {
			while (it.hasNext()) {
				String groupId = it.nextLine();
				long consumerGroupLag = consumerLagService.getConsumerGroupLag(groupId);
				statsDClient.gauge(groupId, consumerGroupLag);
			}
		}
	}

}
