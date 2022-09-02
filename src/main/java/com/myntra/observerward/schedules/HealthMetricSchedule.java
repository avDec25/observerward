package com.myntra.observerward.schedules;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.annotation.Schedules;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class HealthMetricSchedule {

//	@Scheduled(fixedRate = 10, timeUnit = TimeUnit.SECONDS)
//	public void pushHealthMetric() {
//		String prefix = "consumer.lag.random";
//		String hostname = "localhost";
//		int port = 8125;
//		StatsDClient statsDClient = new NonBlockingStatsDClient(prefix, hostname, port);
//
//		SecureRandom secureRandom = new SecureRandom();
//		int delta = secureRandom.nextInt() % 100;
//		log.info("{} at {}", new Date(), delta);
//		statsDClient.gauge("health.metric", delta);
//	}
}
