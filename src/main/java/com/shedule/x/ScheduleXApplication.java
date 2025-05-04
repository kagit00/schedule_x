package com.shedule.x;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
@EnableAsync
@EnableCaching
public class ScheduleXApplication {

	public static void main(String[] args) {
		SpringApplication.run(ScheduleXApplication.class, args);
	}

}
