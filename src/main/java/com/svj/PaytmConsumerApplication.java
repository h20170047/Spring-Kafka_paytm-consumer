package com.svj;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.svj.dto.PaymentRequest;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
@Slf4j
public class PaytmConsumerApplication {
	private ObjectMapper objectMapper= new ObjectMapper();
	@PostConstruct
	public void setup(){
		objectMapper.registerModule(new JavaTimeModule());
	}

	public static void main(String[] args) {
		SpringApplication.run(PaytmConsumerApplication.class, args);
	}

	@KafkaListener(topics = "PAYMENT_TOPIC2", groupId = "Payment_consumer_group")
	public void paymentConsumer1(PaymentRequest paymentRequest) throws JsonProcessingException {
		log.info("PaymentConsumer1 consumed message {}", objectMapper.writeValueAsString(paymentRequest));
	}

	@KafkaListener(topics = "PAYMENT_TOPIC2", groupId = "Payment_consumer_group")
	public void paymentConsumer2(PaymentRequest paymentRequest) throws JsonProcessingException {
		log.info("PaymentConsumer2 consumed message {}", objectMapper.writeValueAsString(paymentRequest));
	}

	@KafkaListener(topics = "PAYMENT_TOPIC2", groupId = "Payment_consumer_group")
	public void paymentConsumer3(PaymentRequest paymentRequest) throws JsonProcessingException {
		log.info("PaymentConsumer3 consumed message {}", objectMapper.writeValueAsString(paymentRequest));
	}

	@KafkaListener(topics = "PAYMENT_TOPIC2", groupId = "Payment_consumer_group")
	public void paymentConsumer4(PaymentRequest paymentRequest) throws JsonProcessingException {
		log.info("PaymentConsumer4 consumed message {}", objectMapper.writeValueAsString(paymentRequest));
	}
}
