package com.myntra.observerward.controller;

import com.myntra.observerward.service.ConsumerLagService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

@Slf4j
@Controller
@RequestMapping("observe")
public class ConsumerLagController {
	@Autowired
	ConsumerLagService consumerLagService;

	@GetMapping("consumer-lag/{groupId}")
	public ResponseEntity<?> getConsumerLag(@PathVariable String groupId) {
		return ResponseEntity.ok().body(consumerLagService.getConsumerGroupLag(groupId));
	}
}
