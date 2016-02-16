'use strict';

var Model = require('../model.js'),
    generateId = require('nodee-utils').shortId.generate,
    mongo = require('./mongoConn.js');

/*
 * MongoDb data source
 * 
 */
var Mongo = module.exports = Model.define('MongoDataSource',['DataSource'], {
    _id:{ }, // same as id
    id:{ }, // string or mongo ObjectId
    deleted:{ isBoolean:true }, // if softRemove
    createdDT:{ date:true },
    modifiedDT: { date:true }, // if optimisticLock
});

/*
 * expose used module methods to use in future
 */
Mongo.connector = mongo;

/*
 * defaults
 */
Mongo.extendDefaults({
    connection:{
        // host: 'localhost',
        // port: 27017,
        // database: 'databaseName',
        // collection: 'collectionName',
        // ...
        indexes: {
            id: { 'id':1, $options:{ unique:true }},
            createdDT: { 'createdDT':1 }
        }
    },
    query:{
        // deleted:{ $ne:true } // default query when softRemove: true
    },
    options:{
        sort:{}, // createdDT:1
        limit: undefined,
        skip: 0,
        fields: {},
        softRemove: false,
        optimisticLock: true, // checking document version on update is enabled by default
        shortId:true // decide if using mongodb ObjectID or shortId, default is shortId
    },
    cache:{
        keyPrefix:'nodee-model-mongo',
        // createKey: createCacheKey - using createKey defined in DataSource
        // duration: 3000, // duration (undefined or zero means no expiration)
        // use: false, // by default do ot use cache
    }
});


/*
 * Constructor methods
 */

/*
 * init method is for index setup, or checking data store connections, etc...
 * init should be run after any new inherited model definition
 */
Mongo.addMethod('init', function(cb){
    var ModelCnst = this;
    
    // cerate mongoUrl
    this._defaults.connection.mongoUrl = mongo.createMongoUrl(this._defaults.connection);
    
    // ensure indexes
    var indexes = [];
    var defIndexes = this.getDefaults().connection.indexes;
    
    for(var key in defIndexes){
        indexes.push(defIndexes[key]);
    }
    
    this.ensureIndex(this.getDefaults().connection, indexes, function(err, indexNames){
        if(err) throw new Error((ModelCnst._name||'MongoDataSource')+' init: index ensuring failed').cause(err);
        // console.log(indexNames);
        if(typeof cb === 'function') cb(null, indexNames);
    });
});

// index creation helper
Mongo.addMethod('ensureIndex', function(connection, index, callback){ // callback(err, indexName)
    var single = false;
    var indexNames = [];
    if(!Array.isArray(index)) {
        index = [index];
        single = true;
    }
    
    ensureIndexes(0);
    function ensureIndexes(i){
        i = i || 0;
        var options = null;
        if(i<index.length) {
            if(index[i].$options) {
                options = index[i].$options;
                delete index[i].$options;
            }
            mongo.getDb(connection.mongoUrl, function(err, db){
                if(err) callback(err);
                else db.collection(connection.collection).ensureIndex(index[i], options, function(err, indexName){
                    if(err) callback(err);
                    else {
                        indexNames.push(indexName);
                        ensureIndexes(i+1);
                    }
                });
            });
        }
        else {
            callback(null, single ? indexNames[0] : indexNames);
        }
    }
});

/*
 * helper for recognizing mongo errors
 */

Mongo.addMethod('parseError', function(err, source){
    if(err && err.code === 11000) { // duplicate key on unique index
        // example err.message:
        // MongoError: insertDocument :: caused by :: 11000 E11000 duplicate key error index: api_exisport_v3.warehouses.$_id_  dup key: { : "asd" }
        // parse message
        var duplValue = err.message.match( /.+dup key: \{ : "(.+)".+/ );
        duplValue = duplValue ? duplValue[1] : '';
        var duplIndex = err.message.match( /.+duplicate key error index: (.+)  dup key.+/ );
        duplIndex = duplIndex ? duplIndex[1] : '';
        
        var validErrs;
        if(duplIndex) {
            validErrs = {};
            validErrs[ duplIndex ] = ['unique'];
            if(duplValue) validErrs[ duplIndex ].push(duplValue);
        }
        
        return new Error((source || 'MongoDataSource') + ': INVALID duplicate value "' +duplValue+ '"').details({ code:'INVALID', validErrs:validErrs, cause:err });
    }
});

/*
 * onFetch - if data modification needed when load from data store (e.g. string to date conversion)
 */
Mongo.onFetch(function(data){
    // id = _id
    data.id = data._id;
});


/*
 * Query builder methods - inherited from DataSource
 */

/*
 * collection().find(...).exec(...) - result is raw data, so do not fetch results
 * data source specific commands (aggregate, upsert, etc...)
 * 
 */
Mongo.Collection.addMethod('exec', { cacheable:true, fetch:false }, function(command, args, cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    mongo.getDb(defaults.connection.mongoUrl, function(err, db){
        if(err) return cb(new Error((ModelCnst._name||'MongoDataSource')+' exec: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var coll = db.collection(defaults.connection.collection);
        try { // in case command will not be recognized
            args.push(function(err, data){
                if(err) return cb(new Error((ModelCnst._name||'MongoDataSource')+' exec: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
                else return cb(null, data);
            });
            coll[command].apply(coll, args);
        }
        catch(err){
            return cb(new Error((ModelCnst._name||'MongoDataSource')+' exec: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        }
    });
});

/*
 * collection().find(...).one(callback) - callback(err, docs) result is single fetched+filled model instance or null
 */
Mongo.Collection.addMethod('one', { cacheable:true, fetch:true }, function(cb){ // cb(err, docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    mongo.getDb(defaults.connection.mongoUrl, function(err, db){
        if(err) return cb(new Error((ModelCnst._name||'MongoDataSource')+' one: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var coll = db.collection(defaults.connection.collection);
        coll.findOne(defaults.query || {}, defaults.options || {}, function(err, data){
            if(err) return cb(new Error((ModelCnst._name||'MongoDataSource')+' one: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
            else return cb(null, data);
        });
    });
});

/*
 * collection().find(...).all(callback) - callback(err, docs) result is array of fetched+filled model instances,
 * if nothing found returns empty array
 */
Mongo.Collection.addMethod('all', { cacheable:true, fetch:true }, function(cb){ // cb(err, docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    mongo.getDb(defaults.connection.mongoUrl, function(err, db){
        if(err) return cb(new Error((ModelCnst._name||'MongoDataSource')+' all: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var coll = db.collection(defaults.connection.collection);
        var stream = coll.find(defaults.query || {}, defaults.options || {}).stream();
        var data = [];
        stream.on('data', function(item) {
            data.push(item);
        });
        stream.on('end', function() {
            cb(null, data);
        });
        stream.on('error', function(err){
            cb(new Error((ModelCnst._name||'MongoDataSource')+' all: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        });
    });
});

/*
 * collection().find(...).count(callback) - callback(err, count) result is count of documents
 */
Mongo.Collection.addMethod('count', { cacheable:true, fetch:false }, function(cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    mongo.getDb(defaults.connection.mongoUrl, function(err, db){
        if(err) return cb(new Error((ModelCnst._name||'MongoDataSource')+' count: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        // limit, skip count does not make sense
        delete defaults.options.limit;
        delete defaults.options.skip;
        
        var coll = db.collection(defaults.connection.collection);
        coll.count(defaults.query || {}, defaults.options || {}, function(err, data){
            if(err) return cb(new Error((ModelCnst._name||'MongoDataSource')+' count: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
            else return cb(null, data);
        });
    });
});

/*
 * collection().find(...).create(data, callback) - callback(err, doc/docs) result is array
 * of created documents, if data is array, else single created document
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
// returns array of created documents if data is array, else single created document
Mongo.Collection.addMethod('create', { cacheable:false, fetch:true }, function(data, cb){ // cb(err, doc/docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    var multiple = true;
    if(!Array.isArray(data)) {
        data = [ data ];
        multiple = false;
    }
    
    var ids = [], now = new Date();
    for(var i=0;i<data.length;i++) {
        data[i].createdDT = data[i].createdDT || now;
        data[i].modifiedDT = data[i].createdDT;
        if(data[i].id) ids.push(data[i].id);
        else data[i].id = defaults.options.shortId ? generateId() : mongo.ObjectID(); // generate new objectId (_id), if it is not set
        
        data[i]._id = data[i].id;
        //if((data[i].id+'').match(/[\$\. ]+/g)) return cb(new Error('MongoDataSource create: INVALID').details({ code:'INVALID', validErrs:{ 'id':['invalid'] } }));
    }
    
    mongo.getDb(defaults.connection.mongoUrl, function(err, db){
        if(err) return cb(new Error((ModelCnst._name||'MongoDataSource')+' create: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var coll = db.collection(defaults.connection.collection);
        coll.insertMany(data, {w:1, safe:true }, function(err, result){
            if(err){
                var mError = ModelCnst.parseError(err, (ModelCnst._name||'MongoDataSource')+' create');
                if(mError) cb(mError);
                else cb(new Error((ModelCnst._name||'MongoDataSource')+' create: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
            }
            else cb(null, multiple ? (result.ops||[]) : (result.ops||[])[0]);
        });
    });
});

/*
 * collection().find(...).update(data, callback) - callback(err, count) result is count of updated documents
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
Mongo.Collection.addMethod('update', { cacheable:false, fetch:false }, function(data, cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    data = data || {};
    
    // be carefull if bulk updating and optimisticLock, need to manual update modifiedDT
    // perform multi document update, if query is not for single document
    if(!defaults.query._id || !defaults.query.id) {
        defaults.options.multi = true;
    }
    
    // can't modify id
    delete data.id;
    delete data._id;
    
    var finalData = {};
    for(var key in data){
        if(key[0]==='$') finalData[key] = data[key];
        else {
            finalData.$set = finalData.$set || {};
            finalData.$set[key] = data[key];
        }
    }
    data = finalData;
    
    mongo.getDb(defaults.connection.mongoUrl, function(err, db){
        if(err) return cb(new Error((ModelCnst._name||'MongoDataSource')+' update: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var coll = db.collection(defaults.connection.collection);
        coll.updateMany(defaults.query, data, defaults.options, function(err, result){
            if(err){
                var mError = ModelCnst.parseError(err, (ModelCnst._name||'MongoDataSource')+' update');
                if(mError) cb(mError);
                else cb(new Error((ModelCnst._name||'MongoDataSource')+' update: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
            }
            else return cb(null, result.modifiedCount || 0);
        });
    });
});

/*
 * collection().find(...).remove(callback) - callback(err, count) result is count of removed documents
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
Mongo.Collection.addMethod('remove', { cacheable:false }, function(cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    mongo.getDb(defaults.connection.mongoUrl, function(err, db){
        if(err) return cb(new Error((ModelCnst._name||'MongoDataSource')+' remove: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var coll = db.collection(defaults.connection.collection);
        coll.deleteMany(defaults.query || {}, {w:1}, function(err, result){
            if(err) return cb(new Error((ModelCnst._name||'MongoDataSource')+' remove: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
            else return cb(null, result.deletedCount || 0);
        });
    });
});


/*
 * Model instance methods - inherited from DataSource
 */