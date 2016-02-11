'use strict';

var model = require('../model.js'),
    generateId = require('nodee-utils').shortId.generate,
    object = require('nodee-utils').object,
    sift = require('nodee-utils').sift;


/*
 * Memory model datasource
 * WARNING: do not use this in production, only for testing
 * 
 */
var Memory = module.exports = model.define('MemoryDataSource',['DataSource'], {
    id:{ isString:true },
    deleted:{ isBoolean:true }, // if softRemove
    createdDT:{ date:true },
    modifiedDT: { date:true }, // if optimisticLock
});

/*
 * defaults
 */
Memory.extendDefaults({
    connection:{
        // collection: 'collectionName'
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
        optimisticLock: true
    },
    cache:{
        keyPrefix:'nodee-model-memory',
        // createKey: createCacheKey - using createKey defined in DataSource
        // duration: 3000, // duration (undefined or zero means no expiration)
        // use: false, // by default do ot use cache
    }
});

/**
 * here are stored data
 * @type {Array}
 */
var memDB = {
    // 'collectionName':[ record1, record2, ... ]
};
Memory.getDB = function(){
    return memDB;
};

// helper for sorting records
function sortArray(array, sort){
    var key = Object.keys(sort)[0];
    var asc = true;
    if(sort[key]=== -1 || sort[key]==='desc') asc = false;
    
    if(key) array.sort(function(a,b){
        if(asc) return (a[key] > b[key]) ? 1 : -1;
        else return (a[key] < b[key]) ? 1 : -1;
    });
    
    return array;
}

/*
 * Constructor methods
 */

/*
 * init method is for index setup, or checking data store connections, etc...
 * init should be run after any new inherited model definition
 */
Memory.addMethod('init', function(){
    // do nothing
    // throw new Error('DataSource: not implemented');
});

/*
 * onFetch - if data modification needed when load from data store (e.g. string to date conversion)
 */
// DataSource.onFetch(function(data){  do something with data here... });


/*
 * Query builder methods - inherited from DataSource
 */


/*
 * collection().find(...).exec(...) - result is raw data, so do not fetch results
 * data source specific commands (aggregate, upsert, etc...)
 * 
 */
Memory.Collection.addMethod('exec', { cacheable:true, fetch:false }, function(command, args, cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    if(!defaults.connection.collection) return cb(new Error('MemoryDataSource: CONNFAIL - defaults.connection.collection is not defined').details({ code:'CONNFAIL' }))
    var records = memDB[ defaults.connection.collection ] = memDB[ defaults.connection.collection ] || [];
    
    var matchedRecords = sift(defaults.query, records);
    
    matchedRecords = sortArray(matchedRecords, defaults.options.sort).slice(defaults.options.skip || 0);
    
    if(defaults.options.limit) matchedRecords = matchedRecords.slice(0, defaults.options.limit);
    
    cb(null, matchedRecords);
});

/*
 * collection().find(...).one(callback) - callback(err, docs) result is single fetched+filled model instance or null
 */
Memory.Collection.addMethod('one', { cacheable:true, fetch:true }, function(cb){ // cb(err, docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    if(!defaults.connection.collection) return cb(new Error('MamoryDataSource: CONNFAIL - defaults.connection.collection is not defined').details({ code:'CONNFAIL' }))
    var records = memDB[ defaults.connection.collection ] = memDB[ defaults.connection.collection ] || [];
    
    var matchedRecords = sift(defaults.query, records);
    
    
    matchedRecords = sortArray(matchedRecords, defaults.options.sort)
                     .slice(defaults.options.skip || 0, (defaults.options.skip || 0)+1);
    cb(null, matchedRecords[0]);
});

/*
 * collection().find(...).all(callback) - callback(err, docs) result is array of fetched+filled model instances,
 * if nothing found returns empty array
 */
Memory.Collection.addMethod('all', { cacheable:true, fetch:true }, function(cb){ // cb(err, docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    if(!defaults.connection.collection) return cb(new Error('MemoryDataSource: CONNFAIL - defaults.connection.collection is not defined').details({ code:'CONNFAIL' }))
    var records = memDB[ defaults.connection.collection ] = memDB[ defaults.connection.collection ] || [];
    
    var matchedRecords = sift(defaults.query, records);
    
    
    matchedRecords = sortArray(matchedRecords, defaults.options.sort).slice(defaults.options.skip || 0);
    if(defaults.options.limit) matchedRecords = matchedRecords.slice(0, defaults.options.limit);
    
    cb(null, matchedRecords);
});

/*
 * collection().find(...).count(callback) - callback(err, count) result is count of documents
 */
Memory.Collection.addMethod('count', { cacheable:true, fetch:false }, function(cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    if(!defaults.connection.collection) return cb(new Error('MemoryDataSource: CONNFAIL - defaults.connection.collection is not defined').details({ code:'CONNFAIL' }))
    var records = memDB[ defaults.connection.collection ] = memDB[ defaults.connection.collection ] || [];
    
    var matchedRecords = sift(defaults.query, records);
    cb(null, matchedRecords.length);
});

/*
 * collection().find(...).create(data, callback) - callback(err, doc/docs) result is array
 * of created documents, if data is array, else single created document
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
// returns array of created documents if data is array, else single created document
Memory.Collection.addMethod('create', { cacheable:false, fetch:true }, function(data, cb){ // cb(err, doc/docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    if(!defaults.connection.collection) return cb(new Error('MemoryDataSource: CONNFAIL - defaults.connection.collection is not defined').details({ code:'CONNFAIL' }))
    var records = memDB[ defaults.connection.collection ] = memDB[ defaults.connection.collection ] || [];
    
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
        else data[i].id = generateId();
    }
    
    // check id duplicity
    if(ids.length>0) ModelCnst.collection().findId(ids).all(function(err, items){
        if(err) cb(new Error('MemoryDataSource create: failed to check id duplicity').cause(err));
        else if(items.length>0) cb(new Error('MemoryDataSource create: one or more document with same id exists').details({ code:'EXECFAIL' }));
        else {
            records = memDB[ defaults.connection.collection ] = records.concat(data);
            cb(null, multiple ? data : data[0]);
        }
    
    });
    else {
        records = memDB[ defaults.connection.collection ] = records.concat(data);
        cb(null, multiple ? data : data[0]);
    }
});

/*
 * collection().find(...).update(data, callback) - callback(err, count) result is count of updated documents
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
Memory.Collection.addMethod('update', { cacheable:false, fetch:false }, function(data, cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    if(!defaults.connection.collection) return cb(new Error('MemoryDataSource: CONNFAIL - defaults.connection.collection is not defined').details({ code:'CONNFAIL' }))
    var records = memDB[ defaults.connection.collection ] = memDB[ defaults.connection.collection ] || [];
    
    data = data || {};
    
    // be carefull if bulk updating and optimisticLock, need to manual update modifiedDT
    // can't modify id
    delete data.id;
    
    var toUpdate = sift(defaults.query, records);
    var count = 0;
    
    for(var i=0;i<toUpdate.length;i++) {
        for(var r in records){
            if(records[r].id === toUpdate[i].id) {
                records[r] = memDB[ defaults.connection.collection ][r] = object.update(records[r], data);
                count++;
                break;
            }
        }
    }
    
    cb(null, count);
});

/*
 * collection().find(...).remove(callback) - callback(err, count) result is count of removed documents
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
Memory.Collection.addMethod('remove', { cacheable:false }, function(cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    if(!defaults.connection.collection) return cb(new Error('MemoryDataSource: CONNFAIL - defaults.connection.collection is not defined').details({ code:'CONNFAIL' }))
    var records = memDB[ defaults.connection.collection ] = memDB[ defaults.connection.collection ] || [];
    
    var toDelete = sift(defaults.query, records);
    var count = 0;
    for(var i=0;i<toDelete.length;i++) {
        for(var r in records){
            if(records[r].id === toDelete[i].id) {
                records.splice(r, 1);
                count++;
                break;
            }
        }
    }
    
    cb(null, count);
});


/*
 * Model instance methods - inherited from DataSource
 */