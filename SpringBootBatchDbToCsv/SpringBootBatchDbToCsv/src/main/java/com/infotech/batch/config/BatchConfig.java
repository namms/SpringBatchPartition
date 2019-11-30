package com.infotech.batch.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import com.infotech.batch.model.Person;
import com.infotech.batch.processor.PersonItenProcessor;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private DataSource dataSource;
	private int i=0;

	@Bean
	@StepScope
	public JdbcCursorItemReader<Person> reader(@Value("#{stepExecutionContext[fromId]}") final String fromId){
		System.out.println(fromId);
		i++;
		JdbcCursorItemReader<Person> cursorItemReader = new JdbcCursorItemReader<>();
		cursorItemReader.setDataSource(dataSource);
		cursorItemReader.setSql("SELECT person_id,first_name,last_name,email,age FROM person");
		cursorItemReader.setRowMapper(new PersonRowMapper());

		return cursorItemReader;
	}

	@Bean
	@StepScope
	public PersonItenProcessor processor(){
		return new PersonItenProcessor();
	}

	@Bean
	@StepScope
	public FlatFileItemWriter<Person> writer(@Value("#{stepExecutionContext[fromId]}") final String fromId){
		FlatFileItemWriter<Person> writer = new FlatFileItemWriter<Person>();

		writer.setResource(new FileSystemResource("csv/outputs/users.processed" + "-" + fromId + ".csv"));
		//writer.setAppendAllowed(true);
		//writer.setShouldDeleteIfEmpty(true);
		//writer.setShouldDeleteIfExists(true);
		DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<Person>();
		lineAggregator.setDelimiter(",");
		
		BeanWrapperFieldExtractor<Person>  fieldExtractor = new BeanWrapperFieldExtractor<Person>();
		fieldExtractor.setNames(new String[]{"personId","firstName","lastName","email","age"});
		lineAggregator.setFieldExtractor(fieldExtractor);
		
		writer.setLineAggregator(lineAggregator);
		return writer;
	}
	
	@Bean
	public Step step1(){
		return stepBuilderFactory.get("step1")
				.<Person,Person>chunk(100)
				.reader(reader(null))
				.processor(processor())
				.writer(writer(null))
				.build();
	}
	@Bean
	public Step stepManager() {
		return stepBuilderFactory.get("stepManager")
				.partitioner(step1().getName(), rangePartitioner())
				.partitionHandler(partitionHandler()).build();
	}

	@Bean
	public PartitionHandler partitionHandler() {
		TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
		handler.setGridSize(10);
		handler.setTaskExecutor(taskExecutor());
		handler.setStep(step1());
		try {
			handler.afterPropertiesSet();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return handler;
	}
	@Bean
	public SimpleAsyncTaskExecutor taskExecutor() {
		return new SimpleAsyncTaskExecutor();
	}

	@Bean
	public Job exportPerosnJob(){
		return jobBuilderFactory.get("exportPeronJob")
				.incrementer(new RunIdIncrementer())
				.flow(stepManager())
				.end()
				.build();
	}
	@Bean
	public RangePartitioner rangePartitioner() {
		return new RangePartitioner();
	}
}
