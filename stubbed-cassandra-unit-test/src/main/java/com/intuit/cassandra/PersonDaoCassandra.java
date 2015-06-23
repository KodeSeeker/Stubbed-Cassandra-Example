/**
 *
 */
package com.intuit.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PersonDaoCassandra implements PersonDao {

    private int port;
    private int retries;
    private Cluster cluster;
    private Session session;
    private PreparedStatement storeStatement;
    private PreparedStatement retrieveStatement;

    public PersonDaoCassandra(int port, int retries) {
        this.port = port;
        this.retries = retries;
    }

    public void connect() {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReadTimeoutMillis(500);
        cluster = Cluster.builder()   //.addContactPoints("localhost", "localhost")
                .addContactPoint("localhost")
                .withPort(port)
                .withRetryPolicy(new LoggingRetryPolicy(new RetryReads()))
                .withSocketOptions(socketOptions)
                .build();
        session = cluster.connect("people");
        storeStatement = session.prepare("insert into person(name, age, interesting_dates) values (?,?,?)");
        retrieveStatement = session.prepare("select * from person where name = ?");
    }

    public void disconnect() {
        cluster.close();
    }

    public List<Person> retrievePeople() {
        ResultSet result;
        try {
            Statement statement = new SimpleStatement("select * from person");
            statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            result = session.execute(statement);
        } catch (ReadTimeoutException e) {
            throw new UnableToRetrievePeopleException();
        }

        List<Person> peopleList = new ArrayList<Person>();
        for (Row row : result) {
            peopleList.add(new Person(row.getString("first_name"), row.getInt("age"), null));
        }
        
        return peopleList;
    }

    
    public List<Person> retrievePeopleByName(String firstName) {
        BoundStatement bind = retrieveStatement.bind(firstName);
        ResultSet result = session.execute(bind);

        List<Person> people = new ArrayList<Person>();
        for (Row row : result) {
            people.add(new Person(row.getString("name"), row.getInt("age"), row.getList("interesting_dates", Date.class)));
        }
        return people;
    }

    
    public void storePerson(Person person) {
        try {
            BoundStatement bind = storeStatement.bind(person.getName(), person.getAge(), person.getInterestingDates());
            session.execute(bind);
        } catch (NoHostAvailableException e) {
            throw new UnableToSavePersonException();
        }
    }

    private class RetryReads implements RetryPolicy {
        
        public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            if (nbRetry < retries) {
                return RetryDecision.retry(ConsistencyLevel.ONE);
            } else {
                return RetryDecision.rethrow();
            }
        }

        
        public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            return DefaultRetryPolicy.INSTANCE.onWriteTimeout(statement, cl, writeType, receivedAcks, receivedAcks, nbRetry);
        }

        
        public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            return DefaultRetryPolicy.INSTANCE.onUnavailable(statement, cl, requiredReplica, aliveReplica, nbRetry);
        }
    }

}
