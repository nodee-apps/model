'use strict';

/*
 * using mongodb driver >= 2.0
 * docs: http://mongodb.github.io/node-mongodb-native/2.0/api-docs/
 */
var MongoClient = require('mongodb').MongoClient,
    ObjectID = require('mongodb').ObjectID,
    promise = require('nodee-utils').async.Series.promise;

/*
 * Small wrapper around mongo native driver, will get Db, or creates connection if
 */
module.exports = {
    getClient: getClient,
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
    // opts.connString,
    // opts.host,
    // opts.database,
    // opts.username,
    // opts.password,
    opts.host = opts.host || 'localhost';
    opts.port = opts.port || 27017; // default mongodb port
    
    if(!opts.connString && (!opts.host || !opts.database)) {
        throw new Error('Model datasources mongoConnector: connection string, host or database missing');
    }

    var connString = opts.connString || opts.connstring;
    if(connString) {
        if(!getDatabaseDromConnString(connString)) throw new Error('Model datasources mongoConnector: connection string must contains database name');
        return connString;
    }
    
    // only connection prefs are copyied to mongoUrl
    var nonPrefs = ['connName', 'connString', 'connstring', 'host', 'port', 'database', 'collection', 'username', 'password', 'mongoUrl', 'indexes'];
    
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
 * detect if connection string has param
 * @param {String} connString 
 * @param {String} paramName 
 */
function connStringHasParam(connString, paramName){
    return connString.indexOf('?'+paramName+'=') > -1 || connString.indexOf('&'+paramName+'=') > -1;
}

/**
 * append parameter to mongodb connection string
 * @param {String} connString 
 * @param {String} paramName 
 * @param {String} paramValue 
 */
function appendParamToConnString(connString, paramName, paramValue){
    return connString += (connString.indexOf('?') > -1 ? '&' : '?') + paramName + '=' + paramValue;
}

/**
 * parse connection string and return database name
 * @param {String} connString 
 */
function getDatabaseDromConnString(connString){
    return (connString.match(/\:\/\/[^\/]+\/([^\?]*)(.*)$/) || [])[1];
}

/**
 * mongo clients
 * @type {Object}
 */
var mongoDbs = {};

/**
 * gets or creates mongo client
 * @param {String} mongoUrl will be generated if not set
 * @param {Function} callback (err, mongoClient)
 */
function getClient(mongoUrl, callback) { // callback(err, mongoClient)
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
            MongoClient.connect(mongoUrl, { useNewUrlParser: true }, function(err, mongoClient){
                if(err && err.code === 18 && !connStringHasParam(mongoUrl, 'authSource')) {
                    
                    // auth failed, try to check admin database as authSource, if authSource is not defined in options
                    MongoClient.connect( appendParamToConnString(mongoUrl, 'authSource', 'admin'), { useNewUrlParser: true }, function(err, mongoClient){
                        if(err) callback(new Error('Model datasources mongoConnector: connection failed').cause(err));
                        else promise.fulfill(null, mongoClient);
                    });
                }
                else if(err) callback(new Error('Model datasources mongoConnector: connection failed').cause(err));
                else promise.fulfill(null, mongoClient);
            });
        }).then(function(promise, next){
            if(promise.error) callback(new Error('Model datasources mongoConnector: creating mongo client failed').cause(promise.error));
            else callback(null, promise.value);
            next();
        });
    }
}
