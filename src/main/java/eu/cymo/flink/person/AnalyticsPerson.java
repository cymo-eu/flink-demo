package eu.cymo.flink.person;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class AnalyticsPerson {
    private long id;
    private String fullName;
    private int age;
    
    public AnalyticsPerson() {}
    
    public AnalyticsPerson(long id, String fullName, int age) {
        super();
        this.id = id;
        this.fullName = fullName;
        this.age = age;
    }
    
    public long getId() {
        return id;
    }
    public void setId(long id) {
        this.id = id;
    }
    public String getFullName() {
        return fullName;
    }
    public void setFullName(String fullName) {
        this.fullName = fullName;
    }
    public int getAge() {
        return age;
    }
    public void setAge(int age) {
        this.age = age;
    }
    
    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
    
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
    
    public Person toPerson() {
        var nameParts = fullName.split(" ");
        assert nameParts.length > 1 : new IllegalArgumentException("Name is expected to have atleast 2 parts");
        
        var firstName = nameParts[0];
        var lastName = IntStream.range(1, nameParts.length)
                .mapToObj(i -> nameParts[i])
                .collect(Collectors.joining(" "));
        
        return new Person(
                String.valueOf(id),
                firstName,
                lastName,
                age);
    }
}
