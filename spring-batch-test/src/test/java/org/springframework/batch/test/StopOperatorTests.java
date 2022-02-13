/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.junit.Test;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link JobOperator#stop(long)}
 *
 * @author Marvin Deng
 */
public class StopOperatorTests {

	static JobOperator jobOperator;

	static CountDownLatch startStopLatch = new CountDownLatch(1);

	static CountDownLatch finishStopLatch = new CountDownLatch(1);

	/**
	 * Use two signal to simulate the case that the stop command executed after step finish but before job finish.
	 *
	 * @throws Exception
	 */
	@Test
	public void testStop() throws Exception {
		ApplicationContext context = new AnnotationConfigApplicationContext(StopJobConfiguration.class);

		JobLauncherTestUtils testUtils = context.getBean(JobLauncherTestUtils.class);

		jobOperator = context.getBean(JobOperator.class);

		ExecutorService executorService = Executors.newFixedThreadPool(2);

		// Launch job in one thread
		Future<JobExecution> future = executorService.submit(() -> {
			try {
				return testUtils.launchJob();
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		// Stop this job execution in another thread
		executorService.submit(() -> {
			try {
				// Wait startSop signal
				startStopLatch.await();
				jobOperator.stop(0L);
				// Send finishStop signal
				finishStopLatch.countDown();
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		// Expected: this job execution can be stopped
		assertEquals(BatchStatus.STOPPED, future.get().getStatus());
	}

	@Configuration
	@EnableBatchProcessing
	static class StopJobConfiguration {
		@Autowired
		public JobBuilderFactory jobBuilderFactory;

		@Autowired
		public StepBuilderFactory stepBuilderFactory;

		@Bean
		public JobRegistry jobRegistry() {
			return new MapJobRegistry();
		}

		@Bean
		public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor() {
			JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
			jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry());
			return jobRegistryBeanPostProcessor;
		}

		@Bean
		public JobOperator jobOperator(JobLauncher jobLauncher, JobRepository jobRepository,
				JobExplorer jobExplorer, JobRegistry jobRegistry) {
			SimpleJobOperator jobOperator = new SimpleJobOperator();
			jobOperator.setJobExplorer(jobExplorer);
			jobOperator.setJobLauncher(jobLauncher);
			jobOperator.setJobRegistry(jobRegistry);
			jobOperator.setJobRepository(jobRepository);
			return jobOperator;
		}

		@Bean
		public Step step() {
			return stepBuilderFactory.get("step")
					.tasklet((contribution, chunkContext) -> RepeatStatus.FINISHED)
					.stream(new MockStream())
					.build();
		}

		@Bean
		public Job job() {
			return jobBuilderFactory.get("job").start(step()).build();
		}

		@Bean
		public JobLauncherTestUtils testUtils() {
			JobLauncherTestUtils jobLauncherTestUtils = new JobLauncherTestUtils();
			jobLauncherTestUtils.setJob(job());
			return jobLauncherTestUtils;
		}

		@Bean
		public DataSource dataSource() {
			return new EmbeddedDatabaseBuilder()
					.addScript("/org/springframework/batch/core/schema-drop-hsqldb.sql")
					.addScript("/org/springframework/batch/core/schema-hsqldb.sql")
					.generateUniqueName(true)
					.build();
		}
	}

	static class MockStream extends ItemStreamSupport {

		@Override
		public void close() {
			try {
				// Send startStop signal
				startStopLatch.countDown();
				// Wait finishStop signal
				finishStopLatch.await();
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
