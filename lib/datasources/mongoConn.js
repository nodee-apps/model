'use strict';

/*
 * using mongodb driver >= 2.0
 * docs: http://mongodb.github.io/node-mongodb-native/2.0/api-docs/
 */
var MongoClient = require('mongodb').MongoClient,
    Server = require('mongodb').Server,
    ObjectID = require('mongodb').ObjectID,
    promise = require('nodee-utils').async.Series.promise;

/*
 * Small wrapper around mongo native driver, will get Db, or creates connection if
 */
module.exports = {
    getDb: getDb,
    createMongoUrl: createMongoUrl,
    ObjectID: ObjectID
};


/**
 * creates mongoUrl from connection opts
 * @param {Object} opts connection opts
 * @returns {String}  mongoUrl
 */
function createMongoUrl(opts){
    // opts.connName,
    // opts.host,
    // opts.database,
    // opts.username,
    // opts.password,
    opts.host = opts.host || 'localhost';
    opts.auto_reconnect = opts.auto_reconnect === false ? false : true;
    opts.port = opts.port || 27017; // default mongodb port
    opts.poolSize = opts.poolSize || 5; // default mongo use 5 connections per client
    opts.native_parser = opts.native_parser === false ? false : true;
    
    if(!opts.host || !opts.database) {
        throw new Error('Model datasources mongoConnector: connection host or database missing');
    }
    
    // only connection prefs are copyied to mongoUrl
    var nonPrefs = ['connName', 'host', 'port', 'database', 'collection', 'username', 'password', 'mongoUrl', 'indexes'];
    
    // http://docs.mongodb.org/manual/reference/connection-string/
    // mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
    var mongoUrl = 'mongodb://' +( opts.username ? (opts.username+':'+opts.password+'@') : '' ) +
              opts.host+ ':' +opts.port+ '/' + opts.database + '?';
    
    // copy prefs to mongoUrl
    for(var key in opts) if(nonPrefs.indexOf(key)===-1) mongoUrl += key+'='+opts[key]+'&';
    mongoUrl = mongoUrl.substring(0, mongoUrl.length-1); // remove last "&"
    
    return mongoUrl;
}

/**
 * mongo clients
 * @type {Object}
 */
var mongoDbs = {};

/**
 * gets or creates mongo client db
 * @param {String} mongoUrl will be generated if not set
 * @param {Function} callback (err, mongoClient)
 */
function getDb(mongoUrl, callback) { // callback(err, mongoDb)
    if(typeof callback !== 'function') throw new Error('Wrong arguments');
    
    var dbPromise = mongoDbs[mongoUrl];
    if(dbPromise) { // mongo client db for this connection already promised
        if(dbPromise.isPending) {
            dbPromise.then(function(promise, next){
                if(promise.error) callback(new Error('Model datasources mongoConnector: creating mongo client failed').cause(promise.error));
                else callback(null, promise.value);
                next();
            });
        }
        else if(dbPromise.isFulfilled && dbPromise.error) {
            callback(new Error('Model datasources mongoConnector: creating mongo client failed').cause(dbPromise.error));
        }
        else if(dbPromise.isFulfilled){
            callback(null, dbPromise.value);
        }
    }
    else { // create new client, and open connection
        mongoDbs[mongoUrl] = promise(function(promise){
            MongoClient.connect(mongoUrl, function(err, db){
                if(err) callback(new Error('Model datasources mongoConnector: connection failed').cause(err));
                else promise.fulfill(null, db);
            });
        }).then(function(promise, next){
            if(promise.error) callback(new Error('Model datasources mongoConnector: creating mongo client failed').cause(promise.error));
            else callback(null, promise.value);
            next();
        });
    }
}
