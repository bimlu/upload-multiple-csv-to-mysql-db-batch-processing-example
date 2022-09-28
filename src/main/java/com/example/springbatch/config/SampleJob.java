package com.example.springbatch.config;

import com.example.springbatch.model.StudentCsv;
import com.example.springbatch.model.StudentJdbc;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import javax.sql.DataSource;
import java.io.IOException;

@Configuration
public class SampleJob {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    @Qualifier("datasource")
    private DataSource dataSource;

    @Autowired
    @Qualifier("universitydatasource")
    private DataSource universitydatasource;

    @Bean
    public Job chunkJob() {
        return jobBuilderFactory.get("Chunk Job")
                .incrementer(new RunIdIncrementer())
                .start(firstChunkStep())
                .build();
    }

    @Bean
    public Step firstChunkStep() {
        return stepBuilderFactory.get("First Chunk Step")
                .<StudentCsv, StudentJdbc>chunk(3)
                .reader(multiResourceItemReader())
                .writer(jdbcBatchItemWriter())
                .build();
    }

    @Bean
    @StepScope
    public MultiResourceItemReader<StudentCsv> multiResourceItemReader() {

        Resource[] resources = null;
        ResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
        try {
            resources = patternResolver.getResources("input-files/*.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(">>> resources " + resources);
        MultiResourceItemReader<StudentCsv> reader = new MultiResourceItemReader<>();
        reader.setResources(resources);

        FlatFileItemReader<StudentCsv> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setLineMapper(new DefaultLineMapper<StudentCsv>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames("id", "first_name", "last_name", "email");
                        setDelimiter(",");
                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<StudentCsv>() {
                    {
                        setTargetType(StudentCsv.class);
                    }
                });
            }
        });
        flatFileItemReader.setLinesToSkip(1);

        reader.setDelegate(flatFileItemReader);
        return reader;
    }


    @Bean
    public JdbcBatchItemWriter<StudentJdbc> jdbcBatchItemWriter() {

        JdbcBatchItemWriter<StudentJdbc> jdbcBatchItemWriter = new JdbcBatchItemWriter<>();
        jdbcBatchItemWriter.setDataSource(universitydatasource);
        jdbcBatchItemWriter.setSql("INSERT INTO students\n" +
                "(id, first_name, last_name, email)\n" +
                "VALUES(:id, :firstName, :lastName, :email);\n");
        jdbcBatchItemWriter.setItemSqlParameterSourceProvider(
                new BeanPropertyItemSqlParameterSourceProvider<StudentJdbc>());
        return jdbcBatchItemWriter;
    }
}
