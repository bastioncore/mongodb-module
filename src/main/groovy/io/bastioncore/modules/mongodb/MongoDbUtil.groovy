package io.bastioncore.modules.mongodb

import com.mongodb.MongoClient
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress

/**
 *
 */
class MongoDbUtil {

    public static MongoClient buildClient(def configuration){
        def servers = []
        configuration.servers.each {
            servers.add(new ServerAddress(it.host,it.port))
        }
        def opts = []
        if(configuration.authentication){
            opts.add(MongoCredential.createCredential(configuration.authentication.username, configuration.authentication.database, configuration.authentication.password.toCharArray()))
        }
        return new MongoClient(servers,opts)
    }
}
