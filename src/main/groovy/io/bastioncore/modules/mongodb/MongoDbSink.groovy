package io.bastioncore.modules.mongodb

import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import io.bastioncore.core.Configuration
import io.bastioncore.core.components.AbstractSink
import io.bastioncore.core.messages.DefaultMessage
import org.bson.Document
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
/**
 *
 */
@Component
@Scope('prototype')
class MongoDbSink extends AbstractSink{

    private MongoClient mongoClient
    private MongoDatabase database
    private MongoCollection<Document> collection

    void onReceive(def message){
        super.onReceive(message)
        if(message instanceof Configuration){
            def servers = []
            configuration.configuration.servers.each {
                servers.add(new ServerAddress(it.host,it.port))
            }
            mongoClient = new MongoClient(servers)
            database = mongoClient.getDatabase(configuration.configuration.database)
            collection = database.getCollection(configuration.configuration.collection)
        }
    }

    @Override
    DefaultMessage process(DefaultMessage defaultMessage) {
        Document doc = new Document(defaultMessage.getContent())
        collection.insertOne(doc)
        return defaultMessage
    }
}
