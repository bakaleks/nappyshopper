NappyShopper - your ultimate shopping app
========================================

Best results are achieved when used in a workshop on Cassandra data consistency.

To setup do the following in cassandra-cli:  

    create keyspace meetup2_rf1 with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = {replication_factor:1};
    create keyspace meetup2_rf2 with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = {replication_factor:2};  
    create keyspace meetup2_rf3 with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = {replication_factor:3};  
    create keyspace meetup2_rf5 with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = {replication_factor:5};  
    create keyspace meetup2_rf3_dc with placement_strategy = 'org.apache.cassandra.locator.NetworkTopologyStrategy' and strategy_options = {'DC1':3, 'DC2':3};  

Then create column family in each keyspace:  

    create column family shoppingcarts with comparator=UTF8Type and default_validation_class = UTF8Type and key_validation_class = UTF8Type;  


To run NappyShopper:

    $mvn compile ideauidesigner:javac2 exec:java



