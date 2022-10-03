package com.myntra.observerward.schedules;

import com.myntra.observerward.service.ConsumerLagService;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.validator.routines.EmailValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.view.freemarker.FreeMarkerConfigurer;

import javax.mail.internet.MimeMessage;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.isNull;

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

	@Autowired
	JavaMailSender emailSender;

	@Autowired
	FreeMarkerConfigurer cfg;


	@SneakyThrows
	@Scheduled(fixedRate = 10, timeUnit = TimeUnit.SECONDS)
	public void pushConsumerGroupLag() {


		int port = Integer.parseInt(PORT);
		StatsDClient statsDClient = new NonBlockingStatsDClient(PREFIX, HOSTNAME, port);

		try (LineIterator it = FileUtils.lineIterator(new File(DATAFILE_GROUPIDS), "UTF-8")) {
			while (it.hasNext()) {
				String[] data = it.nextLine().split(" ");
				String groupId = data[0];

				// extract more data from file ===========================================
				Integer threshold = null;
				List<String> toClients = new ArrayList<>();
				try {
					threshold = Integer.parseInt(data[1]);
					String[] clientsToNotify = data[2].split(",");
					Collections.addAll(toClients, clientsToNotify);
				} catch (Exception e) {
					threshold = null;
				}
				// =======================================================================


				log.info("Checking lag for: {}", groupId);
				long consumerGroupLag = consumerLagService.getConsumerGroupLag(groupId);
				statsDClient.gauge(groupId, consumerGroupLag);

				if (!isNull(threshold) && consumerGroupLag > threshold) {
					notify(groupId, threshold, toClients, consumerGroupLag);
				}
			}
		}
	}

	private void notify(String groupId, Integer threshold, List<String> toClients, long consumerGroupLag) throws IOException, TemplateException {
		Map<String, Object> emailData = new HashMap<>();
		String subject = String.format("Alert: %s is Lagging", groupId);
		Writer out = new StringWriter();
		Template template = cfg.getConfiguration().getTemplate(
				String.format("%s.html", "consumer-group-lagging")
		);
		emailData.put("timestamp", new Date().toString());
		emailData.put("consumer_group", groupId);
		emailData.put("current_lag", consumerGroupLag);
		emailData.put("acceptable_lag", threshold);
		template.process(emailData, out);
		sendEmail(
				toClients,
				subject,
				out.toString()
		);
	}

	void sendEmail(List<String> toClients, String subject, String htmlMessage) {
		try {
			EmailValidator emailValidator = EmailValidator.getInstance();
			toClients.removeIf(email -> !emailValidator.isValid(email));
			String[] clients = toClients.toArray(new String[0]);
			emailSender.send(mimeMessage -> {
				MimeMessageHelper message = new MimeMessageHelper(mimeMessage, true, "UTF-8");
				message.setFrom("Platform Airbus<platforms@myntra.com>");
				message.setTo(clients);
				message.setSubject(subject);
				message.setText(htmlMessage, true);
			});
			log.info("Email sent to: {}", toClients);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to send email; Reason: " + e.getMessage());
		}
	}

}
