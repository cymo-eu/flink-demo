package eu.cymo.flink.job.datagen;

import java.time.ZoneId;
import java.util.UUID;

import com.github.javafaker.Faker;

import eu.cymo.flink.person.CmsPerson;

public class CmsPersonGenerator {
    private static final Faker faker = new Faker();
    
    public static CmsPerson generate() {
        return new CmsPerson(
                UUID.randomUUID().toString(),
                faker.name().firstName(),
                faker.name().lastName(),
                faker.date().birthday(0, 100)
                    .toInstant()
                    .atZone(ZoneId.of("GMT"))
                    .toLocalDate());
    }
}
