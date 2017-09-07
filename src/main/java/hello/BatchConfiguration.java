package hello;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@EnableBatchProcessing
@SpringBootApplication
@Configuration
public class BatchConfiguration extends DefaultBatchConfigurer {
	private static final int THREAD_COUNT = 100;
	private static final int TASK_COUNT = 20000;

	private static AtomicInteger taskCount = new AtomicInteger(0);
	private static final Logger log = LoggerFactory
			.getLogger(BatchConfiguration.class);

	public static void main(String args[]) {
		SpringApplication.run(BatchConfiguration.class, args);
	}

	@Autowired
	protected JobBuilderFactory jobBuilderFactory;

	@Autowired
	protected StepBuilderFactory stepBuilderFactory;

	@Bean
	protected TaskExecutor taskExecutor() {
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		taskExecutor.setConcurrencyLimit(THREAD_COUNT);
		return taskExecutor;
	}

	@Bean
	protected RestTemplate restTemplate(RestTemplateBuilder builder) {
		SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
		Proxy proxy = new Proxy(Type.HTTP, new InetSocketAddress(
				"10.100.242.90", 8080));
		requestFactory.setProxy(proxy);
		return builder.requestFactory(requestFactory).build();
	}

	@Bean
	protected Job job(@Qualifier("step1") Step step1) {
		return jobBuilderFactory.get("myJob").start(step1).build();
	}

	@Bean
	protected Tasklet requestQuoteTask(RestTemplate restTemplate) {
		return new Tasklet() {

			@Override
			public RepeatStatus execute(StepContribution arg0, ChunkContext arg1)
					throws Exception {
				for (int i = 0; i < TASK_COUNT; i++) {
					Quote quote = restTemplate.getForObject(
							"http://gturnquist-quoters.cfapps.io/api/random",
							Quote.class);
					log.info("task: {} - {}", taskCount.incrementAndGet(),
							quote.toString());
				}
				return RepeatStatus.FINISHED;
			}
		};
	}

	@Bean
	protected Step step1(Tasklet tasklet, TaskExecutor taskExecutor) {
		return stepBuilderFactory.get("step1").tasklet(tasklet)
				.taskExecutor(taskExecutor).build();
	}

}