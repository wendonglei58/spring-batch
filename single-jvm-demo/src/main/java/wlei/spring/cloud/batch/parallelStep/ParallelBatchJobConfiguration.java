package wlei.spring.cloud.batch.parallelStep;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import wlei.spring.cloud.batch.domain.Transaction;
import wlei.spring.cloud.batch.multiThreaded.MultiThreadedJobApplication;

/**
 * @author Wendong Lei
 * @version 1.0
 * @since 8/4/2021
 **/
@EnableBatchProcessing
@SpringBootApplication
@Import(MultiThreadedJobApplication.class)
public class ParallelBatchJobConfiguration {
    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    JdbcBatchItemWriter<Transaction> jdbcBatchItemWriter;

    @Bean
    public FlatFileItemReader<Transaction> flatFileReader(@Value("#{jobParameters['inputFlatFiles']}")Resource file) {
        return new FlatFileItemReaderBuilder<Transaction>()
                .resource(file)
                .name("delimitedReader")
                .delimited().names("account", "amount", "timestamp")
                .fieldSetMapper(fieldSet -> {
                    Transaction tran = new Transaction();
                    tran.setAccount(fieldSet.readString("account"));
                    tran.setAmount(fieldSet.readBigDecimal("amount"));
                    tran.setTimestamp(fieldSet.readDate("timestamp"));
                    return tran;
                }).build();
    }

    @Bean
    public StaxEventItemReader<Transaction> xmlFileReader(@Value("#{jobParameters['inputXmlFile']}") Resource file) {
        Jaxb2Marshaller unmarshaller = new Jaxb2Marshaller();
        unmarshaller.setClassesToBeBound(Transaction.class);

        return new StaxEventItemReaderBuilder()
                .name("xmlReader")
                .resource(file)
                .addFragmentRootElements("transaction")
                .unmarshaller(unmarshaller)
                .build();
    }

    @Bean
    public Step step() {
        return this.stepBuilderFactory.get("flat-to-db")
                .<Transaction, Transaction>chunk(100)
                .reader(flatFileReader(null))
                .writer(this.jdbcBatchItemWriter)
                .build();
    }

    @Bean
    public Step step2() {
        return this.stepBuilderFactory.get("xml-to-db")
                .<Transaction, Transaction>chunk(100)
                .reader(xmlFileReader(null))
                .writer(this.jdbcBatchItemWriter)
                .build();
    }

    @Bean
    public Job parallelStepJob() {
        Flow secondFlow = new FlowBuilder<Flow>("secondFlow")
                .start(step2()).build();
        Flow parallelFlow = new FlowBuilder<Flow>("firstFlow")
                .start(step())
                .split(new SimpleAsyncTaskExecutor())
                .add(secondFlow)
                .build();
        return this.jobBuilderFactory.get("parallelStepJob")
                .start(parallelFlow)
                .end()
                .build();
    }
}
