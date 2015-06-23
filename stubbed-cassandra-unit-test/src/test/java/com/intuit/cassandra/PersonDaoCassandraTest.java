/**
 *
 */
package com.intuit.cassandra;

 import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.*;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.*;
import org.scassandra.http.client.PrimingRequest.Result;
import org.scassandra.http.client.types.ColumnMetadata;
import org.scassandra.junit.ScassandraServerRule;

 import java.util.*;

 import static org.junit.Assert.*;
import static org.scassandra.cql.ListType.list;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.http.client.types.ColumnMetadata.column;
import static org.scassandra.matchers.Matchers.*;
import static org.scassandra.http.client.types.ColumnMetadata.*;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.MapType.*;
import static org.scassandra.cql.SetType.*;
import static org.scassandra.cql.ListType.*;

 /**
  * @author npipaliya
  *
  */
 
 public class PersonDaoCassandraTest {

     @ClassRule
     public static final ScassandraServerRule SCASSANDRA = new ScassandraServerRule();

     @Rule
     public final ScassandraServerRule resetScassandra = SCASSANDRA;

     public static final int CONFIGURED_RETRIES = 1;

     private static final PrimingClient primingClient = SCASSANDRA.primingClient();
     private static final ActivityClient activityClient = SCASSANDRA.activityClient();

     private PersonDaoCassandra underTest;
     //private PersonDaoCassandra underTest1;

     @Before
     public void setup() {
         underTest = new PersonDaoCassandra(8042, CONFIGURED_RETRIES);
         //underTest1 = new PersonDaoCassandra(8043, CONFIGURED_RETRIES);
         underTest.connect();
         //underTest1.connect();
         activityClient.clearAllRecordedActivity();
     }

     @After
     public void after() {
         underTest.disconnect();
         //underTest1.disconnect();
     }

     @Test
     public void shouldConnectToCassandraWhenConnectCalled() {
         //given
         activityClient.clearConnections();
         //when
         underTest.connect();
         //underTest1.connect();
         
         
         //System.out.println("size:: " + activityClient.retrieveConnections().toString());
         
         //then
         assertTrue("Expected at least one connection to Cassandra on connect",
                 activityClient.retrieveConnections().size() > 0);
     }

    @SuppressWarnings("unchecked")
    @Test
     public void testRetrievingOfNames() throws Exception {
         // given
         Map<String, ?> row = ImmutableMap.of(
                 "first_name", "Chris",
                 "last_name", "Batey",
                 "age", 29);
         primingClient.prime(PrimingRequest.queryBuilder()
                 .withQuery("select * from person")
                 .withColumnTypes(column("age", PrimitiveType.INT))
                 .withRows(row)
                 .build());

         //when
         List<Person> names = underTest.retrievePeople();

         //then
         assertEquals(1, names.size());
         assertEquals("Chris", names.get(0).getName());
     }

     @Test(expected = UnableToRetrievePeopleException.class)
     public void testHandlingOfReadRequestTimeout() throws Exception {
         // given
         PrimingRequest primeReadRequestTimeout = PrimingRequest.queryBuilder()
                 .withQuery("select * from person")
                 .withResult(Result.read_request_timeout)
                 .build();
         primingClient.prime(primeReadRequestTimeout);

         //when
         underTest.retrievePeople();

         //then
     }

     @Test
     public void testCorrectQueryIssuedOnConnect() {
         //given
         Query expectedQuery = Query.builder().withQuery("USE people").withConsistency("ONE").build();
         
         //when
         underTest.connect();

         //then
         List<Query> queries = activityClient.retrieveQueries();
         System.out.println("expectedQuery : " + expectedQuery.toString() + ", expectedQuery.getConsistency(): " + expectedQuery.getConsistency());
         System.out.println("list of queries : " + queries.toString());
         System.out.println("true/false: " + queries.contains(expectedQuery.toString()));
         assertTrue(isContainsQuery(expectedQuery, queries));
         //assertTrue("Expected query not executed, actual queries:  " + queries, queries.contains(expectedQuery));
         //Query{query='USE "people"', consistency='ONE'}
         //Query{query='use people', consistency='ONE'}, Query{query='USE "people"', consistency='ONE'}
         /*
          * expectedQuery : Query{query='USE people', consistency='ONE'}, expectedQuery.getConsistency(): ONE
           list of queries : [Query{query='SELECT * FROM system.peers', consistency='ONE'}, Query{query='SELECT * FROM system.local WHERE key='local'', consistency='ONE'}, Query{query='SELECT * FROM system.schema_keyspaces', consistency='ONE'}, Query{query='SELECT * FROM system.schema_columnfamilies', consistency='ONE'}, Query{query='SELECT * FROM system.schema_columns', consistency='ONE'}, Query{query='SELECT * FROM system.peers', consistency='ONE'}, Query{query='SELECT * FROM system.local WHERE key='local'', consistency='ONE'}, 
           Query{query='use people', consistency='ONE'}, Query{query='USE "people"', consistency='ONE'}]
           true/false: false
          */
     }

     //@Test
     public void testCorrectQueryIssuedOnConnectUsingMatcher() {
         //given
         Query expectedQuery = Query.builder().withQuery("USE people").withConsistency("ONE").build();

         //when
         underTest.connect();

         //then
         assertThat(activityClient.retrieveQueries(), containsQuery(expectedQuery));
     }

     @Test
     public void testQueryIssuedWithCorrectConsistency() {
         //given
         Query expectedQuery = Query.builder().withQuery("select * from person").withConsistency("QUORUM").build();

         //when
         underTest.retrievePeople();

          //then
         List<Query> queries = activityClient.retrieveQueries();
         assertTrue("Expected query with consistency QUORUM, found following queries: " + queries,
                 queries.contains(expectedQuery));
     }

     @Test
     public void testQueryIssuedWithCorrectConsistencyUsingMatcher() {
         //given
         Query expectedQuery = Query.builder()
                 .withQuery("select * from person")
                 .withConsistency("QUORUM").build();

         //when
         underTest.retrievePeople();

         //then
         assertThat(activityClient.retrieveQueries(), containsQuery(expectedQuery));
     }

     @Test
     public void testStorePerson() {
         // given
         PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
                 .withQueryPattern(".*person.*")
                 .withVariableTypes(VARCHAR, INT, list(TIMESTAMP))
                 .build();
         primingClient.prime(preparedStatementPrime);
         underTest.connect();
         Date interestingDate = new Date();
         List<Date> interestingDates = Arrays.asList(interestingDate);

         //when
         underTest.storePerson(new Person("Christopher", 29, interestingDates));

         //then
         PreparedStatementExecution expectedPreparedStatement = PreparedStatementExecution.builder()
                 .withPreparedStatementText("insert into person(name, age, interesting_dates) values (?,?,?)")
                 .withConsistency("ONE")
                 .withVariables("Christopher", 29, Arrays.asList(interestingDate))
                 .build();
         assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedPreparedStatement));
     }

     @Test
     public void testRetrievePeopleViaPreparedStatement() {
         // given
         Date today = new Date();
         Map<String, ?> row = ImmutableMap.of(
                 "name", "Chris Batey",
                 "age", 29,
                 "interesting_dates", Lists.newArrayList(today.getTime())
                 );
         @SuppressWarnings("unchecked")
        PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
                 .withQuery("select * from person where name = ?")
                 .withVariableTypes(VARCHAR)
                 .withColumnTypes(column("age", INT), column("interesting_dates", list(TIMESTAMP)))
                 .withRows(row)
                 .build();
         primingClient.prime(preparedStatementPrime);

         //when
         List<Person> names = underTest.retrievePeopleByName("Chris Batey");

         //then
         assertEquals(1, names.size());
         assertEquals("Chris Batey", names.get(0).getName());
         assertEquals(29, names.get(0).getAge());
         assertEquals(Lists.newArrayList(today), names.get(0).getInterestingDates());
     }

     @Test
     public void testRetriesConfiguredNumberOfTimes() throws Exception {
         PrimingRequest readTimeoutPrime = PrimingRequest.queryBuilder()
                 .withQuery("select * from person")
                 .withResult(Result.read_request_timeout)
                 .build();
         primingClient.prime(readTimeoutPrime);

         try {
             underTest.retrievePeople();
         } catch (UnableToRetrievePeopleException e) {
         }

         assertEquals(CONFIGURED_RETRIES + 1, activityClient.retrieveQueries().size());
     }

     @Test(expected = UnableToSavePersonException.class)
     public void testThatSlowQueriesTimeout() throws Exception {
         // given
         PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
                 .withQueryPattern("insert into person.*")
                 .withVariableTypes(VARCHAR, INT, list(TIMESTAMP))
                 .withFixedDelay(1000)
                 .build();
         primingClient.prime(preparedStatementPrime);
         underTest.connect();

         underTest.storePerson(new Person("Christopher", 29, null));
     }

     @Test
     public void testLowersConsistency() throws Exception {
         PrimingRequest readtimeoutPrime = PrimingRequest.queryBuilder()
                 .withQuery("select * from person")
                 .withResult(Result.read_request_timeout)
                 .build();
         primingClient.prime(readtimeoutPrime);

         try {
             underTest.retrievePeople();
         } catch (UnableToRetrievePeopleException e) {
         }

         List<Query> queries = activityClient.retrieveQueries();
         assertEquals("Expected 2 attempts. Queries were: " + queries, 2, queries.size());
         assertEquals(Query.builder()
                 .withQuery("select * from person")
                 .withConsistency("QUORUM").build(), queries.get(0));
         assertEquals(Query.builder()
                 .withQuery("select * from person")
                 .withConsistency("ONE").build(), queries.get(1));
     }
     
     public boolean isContainsQuery(Query expectedQuery, List<Query> queries) {
         
         for(Query query : queries) {
             if(query.toString().equalsIgnoreCase(expectedQuery.toString())) {
                 return true;
             }
         }
         return false;
     }
 }
