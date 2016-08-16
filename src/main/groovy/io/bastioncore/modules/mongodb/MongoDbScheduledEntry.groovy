package io.bastioncore.modules.mongodb

import com.mongodb.MongoClient
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import io.bastioncore.core.Configuration
import io.bastioncore.core.components.AbstractScheduledEntry
import io.bastioncore.core.scripting.BGroovy
import io.bastioncore.core.messages.DefaultMessage
import org.bson.Document
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

@Component
@Scope('prototype')
class MongoDbScheduledEntry extends AbstractScheduledEntry {
    private MongoClient mongoClient
    private MongoDatabase database
    private MongoCollection<Document> collection

    def state

    String field
    Script query


    void onReceive(def message){
        super.onReceive(message)
        if(message instanceof Configuration){
            debug('configuring MongoDB')
            mongoClient = MongoDbUtil.buildClient(configuration.configuration)
            database = mongoClient.getDatabase(configuration.configuration.database)
            collection = database.getCollection(configuration.configuration.collection)
            field = configuration.configuration.field
            query = BGroovy.instance.parseTemplate(configuration.configuration.query)
            state = configuration.configuration.state

        }

    }

    @Override
    void processTick() {

        Binding b = new Binding()
        b.setVariable('field',field)
        b.setVariable('state',state)
        query.setBinding(b)
        String q = query.run()
        debug('Query is '+q)
        def results = collection.find(Document.parse(q))//.sort(sort)
        debug('Results found : '+results.size())
        results.each {
            state = it[field]
            self().tell(new DefaultMessage(it),self())
        }
    }

    @Override
    DefaultMessage process(DefaultMessage defaultMessage) {
       return defaultMessage
    }

    @Override
    void schedule() {
        schedule(configuration.configuration.delay,configuration.configuration.interval)
    }
}
