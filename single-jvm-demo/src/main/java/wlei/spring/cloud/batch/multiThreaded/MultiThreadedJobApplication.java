package wlei.spring.cloud.batch.multiThreaded;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionManager;
import org.springframework.validation.BindException;
import wlei.spring.cloud.batch.domain.Transaction;

import javax.sql.DataSource;

/**
 * Multi-threaded spring batch job
 *
 * @author Wendong Lei
 * @version 1.0
 * @since 7/24/2021
 **/
@Configuration
public class MultiThreadedJobApplication {
    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Bean
    TaskExecutor fixedSizeExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(8);
        executor.setMaxPoolSize(8);
        executor.afterPropertiesSet();
        return executor;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Transaction> flatFileItemReader(@Value("#{jobParameters['inputFlatFile']}") Resource resource) {
        return new FlatFileItemReaderBuilder<Transaction>()
                .name("file-reader")
                .saveState(false)
                .resource(resource)
                .delimited()
                .names(new String[] {"account", "amount", "timestamp"})
                .fieldSetMapper(fieldSet -> {
                    Transaction transaction = new Transaction();
                    transaction.setAccount(fieldSet.readString("account"));
                    transaction.setAmount(fieldSet.readBigDecimal("amount"));
                    transaction.setTimestamp(fieldSet.readDate("timestamp"));
                    return transaction;
                }).build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Transaction> jdbcBatchItemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .beanMapped()
                .sql("INSERT INTO TRANSACTION(ACCOUNT, AMOUNT, TIMESTAMP) VALUES (:account, :amount, :timestamp)")
                .build();
    }

    @Bean
    public Step fileStep() {
        return this.stepBuilderFactory.get("fileImport")
                .<Transaction, Transaction>chunk(100)
                .reader(flatFileItemReader(null))
                .writer(jdbcBatchItemWriter(null))
                .taskExecutor(fixedSizeExecutor())
                .build();
    }

    @Bean
    public Job multiThreadJob() {
        return this.jobBuilderFactory.get("multiThreadJob")
                .start(fileStep())
                .build();
    }

    public static void main(String[] args) {
        String [] newArgs = new String[] {"inputFlatFile=/data/csv/bigtransactions.csv"};

        SpringApplication.run(MultiThreadedJobApplication.class, newArgs);
    }
}
