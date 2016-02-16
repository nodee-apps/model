'use strict';

var Model = require('../model.js'),
    path = require('path'),
    fsExt = require('nodee-utils').fsExt,
    object = require('nodee-utils').object,
    generateId = require('nodee-utils').shortId.generate,
    sift = require('nodee-utils').sift;


/*
 * JsonFile model datasource
 * data are stored in Json file
 * WARNING: do not use it for storing more than hundreds records, it is not designed for 
 * 
 */
var JsonFile = module.exports = Model.define('JsonFileDataSource',['DataSource'], {
    id:{ isString:true },
    deleted:{ isBoolean:true }, // if softRemove
    createdDT:{ date:true },
    modifiedDT: { date:true }, // if optimisticLock
});

/*
 * expose used module methods and helpers to use in future
 */
JsonFile.parseISODates = parseISODates;
JsonFile.stringify = stringify;

/*
 * defaults
 */
JsonFile.extendDefaults({
    connection:{
        filePath:'', // json file, where data are stored
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
        keyPrefix:'nodee-model-jsonfile',
        // createKey: createCacheKey - using createKey defined in DataSource
        // duration: 3000, // duration (undefined or zero means no expiration)
        // use: false, // by default do ot use cache
    }
});

/*
 * Helpers
 */

// read and require file data, cache results and parse dates
function getFileData(filePath, cb){
    fsExt.requireAsync(filePath, { isJson:true, jsonParse:parseISODates, watch:true }, function(err, data){
        if(err) cb(err);
        else cb(null, data);
    });
}

// parse all string in format ISODate("...") dates
function parseISODates(key, value){
    if(typeof value === 'string'){
        var matched = value.match(/^ISODate\("(.+)"\)$/);
        if(matched) return new Date(Date.parse(matched[1]));
        else return value;
    }
    else return value;
}

// pretty stringify include ISODates
function stringify(data){
    // temporary change date toJSON
    var oldDateJSON = Date.prototype.toJSON;
    Date.prototype.toJSON = function(){ return 'ISODate("' +this.toISOString()+ '")'; };
    
    var str = JSON.stringify(data, null, 4);
    
    // change toJSON back to original
    Date.prototype.toJSON = oldDateJSON;
    return str;
}

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
JsonFile.addMethod('init', function(cb){
    var ModelCnst = this;
    
    this.extendDefaults({
        connection:{
            filePath: path.resolve(this.getDefaults().connection.filePath)
        }
    });
    fsExt.existsOrCreate(this.getDefaults().connection.filePath, { data:'[]' }, function(err){
        if(err) throw new Error((ModelCnst._name||'JsonDataSource')+': init failed').cause(err);
        else if(typeof cb === 'function') cb();
    });
});

/*
 * destroy method is for clearing indexes, removing listeners, etc...
 */
JsonFile.addMethod('destroy', function(){
    fsExt.unwatch(this.getDefaults().connection.filePath);
});

/*
 * onFetch - if data modification needed when load from data store (e.g. string to date conversion)
 */
// JsonFile.onFetch(function(data){ });


/*
 * Query builder methods - inherited from DataSource
 */


/*
 * collection().find(...).exec(...) - result is raw data, so do not fetch results
 * data source specific commands (aggregate, upsert, etc...)
 * 
 */
JsonFile.Collection.addMethod('exec', { cacheable:true, fetch:false }, function(command, args, cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    getFileData(defaults.connection.filePath, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'JsonDataSource')+' exec: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var matchedRecords = sift(defaults.query, records);
        
        matchedRecords = sortArray(matchedRecords, defaults.options.sort).slice(defaults.options.skip || 0);
        if(defaults.options.limit) matchedRecords = matchedRecords.slice(0, defaults.options.limit);
        
        cb(null, matchedRecords);
    });
});

/*
 * collection().find(...).one(callback) - callback(err, docs) result is single fetched+filled model instance or null
 */
JsonFile.Collection.addMethod('one', { cacheable:true, fetch:true }, function(cb){ // cb(err, docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    getFileData(defaults.connection.filePath, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'JsonDataSource')+' one: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var matchedRecords = sift(defaults.query, records);
        
        matchedRecords = sortArray(matchedRecords, defaults.options.sort)
                         .slice(defaults.options.skip || 0, (defaults.options.skip || 0)+1);
        cb(null, matchedRecords[0]);
    });
});

/*
 * collection().find(...).all(callback) - callback(err, docs) result is array of fetched+filled model instances,
 * if nothing found returns empty array
 */
JsonFile.Collection.addMethod('all', { cacheable:true, fetch:true }, function(cb){ // cb(err, docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    getFileData(defaults.connection.filePath, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'JsonDataSource')+' all: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var matchedRecords = sift(defaults.query, records);
        
        matchedRecords = sortArray(matchedRecords, defaults.options.sort).slice(defaults.options.skip || 0);
        if(defaults.options.limit) matchedRecords = matchedRecords.slice(0, defaults.options.limit);
        
        cb(null, matchedRecords);
    });
});

/*
 * collection().find(...).count(callback) - callback(err, count) result is count of documents
 */
JsonFile.Collection.addMethod('count', { cacheable:true, fetch:false }, function(cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    getFileData(defaults.connection.filePath, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'JsonDataSource')+' count: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var matchedRecords = sift(defaults.query, records);
        cb(null, matchedRecords.length);
    });
});

/*
 * collection().find(...).create(data, callback) - callback(err, doc/docs) result is array
 * of created documents, if data is array, else single created document
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
// returns array of created documents if data is array, else single created document
JsonFile.Collection.addMethod('create', { cacheable:false, fetch:true }, function(data, cb){ // cb(err, doc/docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    getFileData(defaults.connection.filePath, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'JsonDataSource')+' create: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        
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
        if(ids.length>0) {
            var q = ModelCnst.collection().findId(ids);
            delete q._defaults.query.deleted; // check all, include deleted documents
            q.all(function(err, items){
                if(err) cb(new Error((ModelCnst._name||'JsonDataSource')+' create: failed to check id duplicity').cause(err));
                else if(items.length>0) cb(new Error((ModelCnst._name||'JsonDataSource')+' create: one or more document with same id exists').details({ code:'EXECFAIL' }));
                else {
                    records = records.concat(data);
                    // update json file
                    fsExt.writeFile(defaults.connection.filePath, stringify(records), function(err){
                        if(err) cb(new Error((ModelCnst._name||'JsonDataSource')+' create: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
                        else cb(null, multiple ? data : data[0]);
                    });
                }
            });
        }
        else {
            records = records.concat(data);
            // update json file
            fsExt.writeFile(defaults.connection.filePath, stringify(records), function(err){
                if(err) cb(new Error((ModelCnst._name||'JsonDataSource')+' create: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
                else cb(null, multiple ? data : data[0]);
            });
        }
    });
});

/*
 * collection().find(...).update(data, callback) - callback(err, count) result is count of updated documents
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
JsonFile.Collection.addMethod('update', { cacheable:false, fetch:false }, function(data, cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    data = data || {};
    getFileData(defaults.connection.filePath, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'JsonDataSource')+' update: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        // be carefull if bulk updating and optimisticLock, need to manual update modifiedDT
        // can't modify id
        delete data.id;
        
        var toUpdate = sift(defaults.query, records);
        var count = 0;
        
        for(var i=0;i<toUpdate.length;i++) {
            for(var r in records){
                if(records[r].id === toUpdate[i].id) {
                    records[r] = object.update(records[r], data);
                    count++;
                    break;
                }
            }
        }
        
        // update json file
        fsExt.writeFile(defaults.connection.filePath, stringify(records), function(err){
            if(err) cb(new Error((ModelCnst._name||'JsonDataSource')+' update: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
            else cb(null, count);
        });
    });
});

/*
 * collection().find(...).remove(callback) - callback(err, count) result is count of removed documents
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
JsonFile.Collection.addMethod('remove', { cacheable:false }, function(cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    getFileData(defaults.connection.filePath, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'JsonDataSource')+' remove: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
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
        
        // update json file
        fsExt.writeFile(defaults.connection.filePath, stringify(records), function(err){
            if(err) cb(new Error((ModelCnst._name||'JsonDataSource')+' remove: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
            else cb(null, count);
        });
    });
});


/*
 * Model instance methods - inherited from DataSource
 */