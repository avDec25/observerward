package com.myntra.observerward.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
public class ConsumerLagService {

	@Autowired
	AdminClient adminClient;

	@Autowired
	KafkaConsumer<String, String> kafkaConsumer;

	@SneakyThrows
	public Long getConsumerGroupLag(String producerApp, String eventName, String consumerApp) {
		String groupId = getGroupId(producerApp, eventName, consumerApp);
		return analyzeLag(groupId);
	}

	@SneakyThrows
	public Long getConsumerGroupLag(String groupId) {
		return analyzeLag(groupId);
	}

	private String getGroupId(String producerApp, String eventName, String consumerApp) {
		return String.format("%s-%s-%s-consumer-group", consumerApp, producerApp, eventName);
	}

	private Map<TopicPartition, Long> getConsumerGrpOffsets(String groupId) throws ExecutionException, InterruptedException {
		ListConsumerGroupOffsetsResult info = adminClient.listConsumerGroupOffsets(groupId);
		Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = info.partitionsToOffsetAndMetadata().get();

		Map<TopicPartition, Long> groupOffset = new HashMap<>();
		for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicPartitionOffsetAndMetadataMap.entrySet()) {
			TopicPartition key = entry.getKey();
			OffsetAndMetadata metadata = entry.getValue();
			groupOffset.putIfAbsent(new TopicPartition(key.topic(), key.partition()), metadata.offset());
		}
		return groupOffset;
	}


	private Map<TopicPartition, Long> getProducerOffsets(Map<TopicPartition, Long> consumerGrpOffset) {
		List<TopicPartition> topicPartitions = new LinkedList<>();
		for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffset.entrySet()) {
			TopicPartition key = entry.getKey();
			topicPartitions.add(new TopicPartition(key.topic(), key.partition()));
		}
		return kafkaConsumer.endOffsets(topicPartitions);
	}

	private Map<TopicPartition, Long> computeLags(Map<TopicPartition, Long> consumerGrpOffsets,
												  Map<TopicPartition, Long> producerOffsets) {
		Map<TopicPartition, Long> lags = new HashMap<>();
		for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffsets.entrySet()) {
			Long producerOffset = producerOffsets.get(entry.getKey());
			Long consumerOffset = consumerGrpOffsets.get(entry.getKey());
			long lag = Math.abs(producerOffset - consumerOffset);
			lags.putIfAbsent(entry.getKey(), lag);
		}
		return lags;
	}

	private Long analyzeLag(String groupId) throws ExecutionException, InterruptedException {
		Map<TopicPartition, Long> consumerGrpOffsets = getConsumerGrpOffsets(groupId);
		Map<TopicPartition, Long> producerOffsets = getProducerOffsets(consumerGrpOffsets);
		Map<TopicPartition, Long> lags = computeLags(consumerGrpOffsets, producerOffsets);
		Long totalLag = 0L;
		for (Map.Entry<TopicPartition, Long> lagEntry : lags.entrySet()) {
			String topic = lagEntry.getKey().topic();
			int partition = lagEntry.getKey().partition();
			Long lag = lagEntry.getValue();
			log.info("Lag for topic = {}, partition = {} is {}", topic, partition, lag);
			totalLag += lag;
		}
		return totalLag;
	}

}
