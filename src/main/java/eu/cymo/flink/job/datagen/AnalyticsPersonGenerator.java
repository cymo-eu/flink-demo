package eu.cymo.flink.job.datagen;

import com.github.javafaker.Faker;

import eu.cymo.flink.person.AnalyticsPerson;

public class AnalyticsPersonGenerator {
    private static final Faker faker = new Faker();

    public static AnalyticsPerson generate() {
        return new AnalyticsPerson(
                faker.number().randomNumber(),
                faker.name().fullName(),
                faker.number().numberBetween(0, 100));
    }
    
}
