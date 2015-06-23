/**
 *
 */
package com.intuit.cassandra;

/**
 * @author npipaliya
 *
 */
import java.util.List;

public interface PersonDao {
    void connect();

    void disconnect();

    List<Person> retrievePeople();

    List<Person> retrievePeopleByName(String firstName);

    void storePerson(Person person);
}
