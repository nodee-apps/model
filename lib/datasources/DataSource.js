'use strict';

var Model = require('../model.js'),
    generateId = require('nodee-utils').shortId.generate,
    cache = require('../cache.js');

/*
 * this Model is like "template" for implementing datasource model-adapter (mongodb, filesystem, ...)
 *
 * error codes:
 * INVALID - doc is not valid, error has validErrs property
 * NOTFOUND - doc or doc state not found (if optimistic concurrency)
 * EXECFAIL - datasource returned exception
 * CONNFAIL - datasource connection fail
 * 
 */

var DataSource = module.exports = Model.define('DataSource', {
    //id:{ isString:true },
    //deleted:{ isBoolean:true }, // if softRemove
    //createdDT:{ date:true },
    //modifiedDT: { date:true }, // if optimisticLock
});

/*
 * defaults
 */
DataSource.setDefaults({
    // connection:{ }, // connection prop, such as database name, or baseUrl when rest, etc...
    query:{
        // deleted:{ $ne:true } // default query when softRemove: true
    },
    options:{
        sort:{}, // createdDT:1
        limit: undefined,
        skip: 0,
        fields: {},
        softRemove: false,
        optimisticLock: false
    },
    cache:{
        keyPrefix:'nodee-model',
        createKey: createCacheKey
        //duration: 3000, // duration (undefined or zero means no expiration)
        //use: false, // by default do ot use cache
    },
    singleInstanceOp: undefined // temp prop to identify source of bulk operations
});
    
/**
 * Method to generate cache key
 * @param {Object} defaults query defaults
 * @param {String} returnedDataType type of data returned by query method e.g. 'single','one','exec'
 * @returns {String}  query unique cache key
 */
function createCacheKey(modelName, defaults, collectionMethod) {
    if(!defaults.cache.keyPrefix) throw new Error('DataSource: cache key have to be defined !');
    if(!modelName) throw new Error('DataSource: model name have to be defined !');
    
    return defaults.cache.keyPrefix + '_' +
        modelName + '_' +
        collectionMethod + '_' +
        JSON.stringify(defaults.db) + '_' +
        JSON.stringify(defaults.query) + '_' +
        JSON.stringify(defaults.options.sort) + '_' +
        JSON.stringify(defaults.options.skip) + '_' +
        JSON.stringify(defaults.options.limit) + '_' +
        JSON.stringify(defaults.options.fields);
}
    
/*
 * Constructor methods
 */

/*
 * init method is for index setup, or checking data store connections, etc...
 * init should be run after any new inherited model definition
 */
DataSource.addMethod('init', function(){
    throw new Error('DataSource: not implemented');
});

/*
 * destroy method is for clearing indexes, removing listeners, etc...
 */
DataSource.addMethod('destroy', function(){
    throw new Error('DataSource: not implemented');
});

/*
 * onFetch - if data modification needed when load from data store (e.g. string to date conversion)
 */
// DataSource.onFetch(function(data){  do something with data here... });


/*
 * Query builder methods
 */

/*
 * collection().find(...)
 */
DataSource.Collection.addMethod('find', function(query){
    query = query || {};
    this.extendDefaults({query:query});
    return this;
});

/*
 * collection().findId(...)
 */
DataSource.Collection.addMethod('findId', function(id){
    var q_id = [];
    if(Array.isArray(id)){
        for(var i=0;i<id.length;i++) {
            q_id.push(id[i]);
        }
        q_id = q_id.length > 0 ? { $in:q_id } : ''; // $in:['id1','id2']
    }
    else {
        q_id = id;
    }
    this.extendDefaults({query:{ id:q_id }});
    return this;
});

/*
 * collection().fields(...)
 */
DataSource.Collection.addMethod('fields', function(fields){
    if(fields){
        //fields.id = fields.id || 1; // show id is default
        fields = fields || {};
        this.extendDefaults({options:{fields:fields}});
    }
    return this;
});

/*
 * collection().sort/order(...)
 */
DataSource.Collection.addMethod(['sort','order'], function(sort){
    if(Object.prototype.toString.call(sort) === '[object Object]'){
        for(var key in sort){
            if(Object.prototype.toString.call(sort[key]) === '[object Object]') sort[key] = sort[key];
            else if(sort[key]==='asc' || sort[key] === 1 || sort[key] === '1') sort[key] = 1;
            else if(sort[key]==='desc' || sort[key] === -1 || sort[key] === '-1') sort[key] = -1;
            else delete sort[key];
        }
        this._defaults.options.sort = sort;
    }
    return this;
});

/*
 * collection().limit/take(...)
 */
DataSource.Collection.addMethod(['limit', 'take'], function(limit){
    limit = limit || 0;
    this._defaults.options.limit = limit;
    return this;
});

/*
 * collection().skip(...)
 */
DataSource.Collection.addMethod('skip', function(skip){
    skip = skip || 0;
    this._defaults.options.skip = skip;
    return this;
});

/*
 * collection().cache(...)
 * duration as integer - miliseconds
 * durations as "30s" = 30 seconds
 * durations as "15m" = 15 minutes
 * durations as "1h" = 1 hour
 */
DataSource.Collection.addMethod('cache', function(duration){
    var ModelCnst = this.getModelConstructor();
    
    if(duration && typeof duration === 'string'){
        var timeType = duration[ duration.length-1 ];
        duration = parseInt(duration, 10);
        if(timeType==='s') duration = duration*1000;
        else if(timeType==='m') duration = duration*60*1000;
        else if(timeType==='h') duration = duration*60*60*1000;
    }
    if(arguments.length && typeof duration !== 'number') throw new Error((ModelCnst._name || 'Datasource') + ' cache: duration is not number, or in format "60s" or "10m" or "1h"');
    
    duration = duration || this._defaults.cache.duration || undefined;
    if(duration) this.extendDefaults({cache:{duration:duration, use:true}});
    return this;
});

/*
 * Query Data Methods
 */

/*
 * collection().clearCache/resetCache(...)
 */
DataSource.Collection.addMethod(['clearCache', 'resetCache'], function(collectionMethod, callback){
    var ModelCnst = this.getModelConstructor();
    
    var key = this.getDefaults().cache.createKey(ModelCnst._name, this.getDefaults(), collectionMethod);
    (Model.cache || cache).del(key, callback);
});
    
/*
 * Draft Methods - need implement datasource specific CRUD
 */

/*
 * collection().find(...).exec(...) - result is raw data, so do not fetch results
 * data source specific commands (aggregate, upsert, etc...)
 * 
 */
DataSource.Collection.addMethod('exec', { cacheable:true, fetch:false }, function(command, args, cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    cb(null, {});
});

/*
 * collection().find(...).exists(callback) - callback(err, exists) result is true/false
 * it is not safe to cache result
 *
 * helper for checking if record exists, it will use collection().one method, but return only boolean (!!data)
 * if data store has method for exists, you can override this
 */
DataSource.Collection.addMethod('exists', { cacheable:false, fetch:false }, function(cb){ // cb(err, exists)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    query.fields({ id:1 }).one(function(err, data){ // no need to return whole document, just "id" field
        if(err) cb(new Error((ModelCnst._name||'DataSource') + '.exists: checking if exists failed').cause(err));
        else cb(null, !!data);
    });
});

/*
 * collection().find(...).one(callback) - callback(err, docs) result is single fetched+filled model instance or null
 */
DataSource.Collection.addMethod('one', { cacheable:true, fetch:true }, function(cb){ // cb(err, docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
        
    cb(null, {});
});

/*
 * collection().find(...).all(callback) - callback(err, docs) result is array of fetched+filled model instances,
 * if nothing found returns empty array
 */
DataSource.Collection.addMethod('all', { cacheable:true, fetch:true }, function(cb){ // cb(err, docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
        
    cb(null, []);
});

/*
 * collection().find(...).count(callback) - callback(err, count) result is count of documents
 */
DataSource.Collection.addMethod('count', { cacheable:true, fetch:false }, function(cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    cb(null, 0);
});

/*
 * collection().find(...).create(data, callback) - callback(err, doc/docs) result is array
 * of created documents, if data is array, else single created document
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
// returns array of created documents if data is array, else single created document
DataSource.Collection.addMethod('create', { cacheable:false, fetch:true }, function(data, cb){ // cb(err, doc/docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    cb(null, {});
});

/*
 * collection().find(...).update(data, callback) - callback(err, count) result is count of updated documents
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
DataSource.Collection.addMethod('update', { cacheable:false, fetch:false }, function(data, cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    // be carefull if bulk updating and optimisticLock, need to manual update modifiedDT
    cb(null, 0);
});

/*
 * collection().find(...).remove(callback) - callback(err, count) result is count of removed documents
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
DataSource.Collection.addMethod('remove', { cacheable:false }, function(cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    cb(null, 0);
});


/*
 * Model instance methods
 */

/*
 * instance.create(callback) - callback(err, doc)
 */
DataSource.prototype.create =
DataSource.wrapHooks('create', function(callback){ // callback(err, doc)
    if(typeof callback !== 'function') throw new Error('callback is required');
    var model = this;
    
    if(!model.id) model.id = generateId();
    
    // if model is in initial state, we have to validate it
    if(model.isValid() === undefined) model.validate();
    
    if(!model.isValid()) {
        callback(new Error((model.constructor._name||'DataSource') + '.prototype.create: INVALID').details({
            code:'INVALID',
            validErrs: model.validErrs()
        }));
    }
    else {
        model.opDefaults({ singleInstanceOp:'create' });
        model.constructor.collection(model.opDefaults()).create(model.getData(), callback);
    }
});

/*
 * instance.update(callback) - callback(err, doc)
 */
DataSource.prototype.update =
DataSource.wrapHooks('update', function(callback){ // callback(err, doc)
    if(typeof callback !== 'function') throw new Error('callback is required');
    var model = this,
        optimisticLock = (model.constructor.getDefaults().options || {}).optimisticLock;
    
    // if model is in initial state, we have to validate it
    if(model.isValid() === undefined) model.validate();
    
    if(!model.isValid()) {
        callback(new Error((model.constructor._name||'DataSource')+'.prototype.update: INVALID').details({
            code:'INVALID', validErrs: model.validErrs() }));
    }
    else if(!model.id) {
        callback(new Error((model.constructor._name||'DataSource')+'.prototype.update: INVALID - missing "id"').details({
            code:'INVALID', validErrs:{ id:[ 'required' ] } }));
    }
    else if(optimisticLock && !model.modifiedDT){
        callback(new Error((model.constructor._name||'DataSource')+'.prototype.update: INVALID - missing "modifiedDT"').details({
            code:'INVALID', validErrs:{ modifiedDT:[ 'required' ] } }));
    }
    else {
        var query = { id: model.id };
        if(optimisticLock) query.modifiedDT = model.modifiedDT;
        model.opDefaults({ singleInstanceOp:'update' });
        
        model.modifiedDT = new Date(); // set modifiedDT
        model.constructor.collection(model.opDefaults()).find(query).update(model.getData(), function(err, count){
            if(err) callback(err);
            else if(count===1) {
                model.setValid(undefined); // clear valid state to trigger validation on next update
                callback(null, model);
            }
            else callback(new Error((model.constructor._name||'DataSource')+'.prototype.update: NOTFOUND "' +JSON.stringify(query)+ '"').details({ code:'NOTFOUND' }));
        });
    }
});

/*
 * instance.remove(callback) - callback(err)
 */
DataSource.prototype.remove =
DataSource.wrapHooks('remove', function(callback){ // callback(err)
    if(typeof callback !== 'function') throw new Error('callback is required');
    var model = this,
        softRemove = (model.constructor.getDefaults().options || {}).softRemove,
        optimisticLock = (model.constructor.getDefaults().options || {}).optimisticLock;
    
    // if model is in initial state, we have to validate it
    if(model.isValid() === undefined) model.validate();
    
    if(!model.isValid()) {
        callback(new Error((model.constructor._name||'DataSource')+'.prototype.remove: INVALID').details({
            code:'INVALID',
            validErrs: model.validErrs()
        }));
    }
    else if(!model.id) {
        callback(new Error((model.constructor._name||'DataSource')+'.prototype.remove: INVALID - missing "id"').details({
            code:'INVALID', validErrs:{ id:[ 'required' ] } }));
    }
    else if(optimisticLock && !model.modifiedDT){
        callback(new Error((model.constructor._name||'DataSource')+'.prototype.remove: INVALID - missing "modifiedDT"').details({
            code:'INVALID', validErrs:{ modifiedDT:[ 'required' ] } }));
    }
    else {
        var query = { id: model.id };
        if(optimisticLock) query.modifiedDT = model.modifiedDT;
        if(softRemove) model.deleted = true;
        model.opDefaults({ singleInstanceOp:'remove' });
        
        if(softRemove) model.constructor.collection(model.opDefaults()).find(query).update({ deleted:true }, function(err, count){
            if(err) callback(err);
            else if(count===1) callback(null, model);
            else callback(new Error((model.constructor._name||'DataSource')+'.prototype.remove: NOTFOUND "' +JSON.stringify(query)+ '"').details({ code:'NOTFOUND' }));
        });
        else model.constructor.collection(model.opDefaults()).find(query).remove(function(err, count){
            if(err) callback(err);
            else if(count===1) callback(null, model);
            else callback(new Error((model.constructor._name||'DataSource')+'.prototype.remove: NOTFOUND "' +JSON.stringify(query)+ '"').details({ code:'NOTFOUND' }));
        });
    }
});