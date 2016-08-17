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
            debug('configuring MongoDB')
            mongoClient = MongoDbUtil.buildClient(configuration.configuration)
            database = mongoClient.getDatabase(configuration.configuration.database)
            collection = database.getCollection(configuration.configuration.collection)
        }
    }

    @Override
    DefaultMessage process(DefaultMessage defaultMessage) {
        debug('Processing document')
        def content = defaultMessage.getContent()
        Document doc
        if(content instanceof Map)
            doc = new Document(content)
        if(content instanceof String)
            doc = Document.parse(content)
        collection.insertOne(doc)
        return defaultMessage
    }
}
