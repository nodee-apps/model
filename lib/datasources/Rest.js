'use strict';

var Model = require('../model.js'),
    generateId = require('enterprise-utils').shortId.generate,
    object = require('enterprise-utils').object,
    async = require('enterprise-utils').async,
    request = require('enterprise-utils').request;

/*
 * Rest data source
 */
var Rest = module.exports = Model.define('RestDataSource',['DataSource'], {
    id:{ }, // string or object
    
    // This will implement inherited rest data source, e.g. elasticSearch
    // deleted:{ isBoolean:true }, // if softRemove
    // createdDT:{ date:true },
    // modifiedDT: { date:true }, // if optimisticLock
});

/*
 * defaults
 */
Rest.extendDefaults({
    connection:{
        endpointUrl:'', // 'http://api.endpoint.com',
        resourceName:'', // resourceName, e.g. "product","customer"
        
        // default headers for every request
        headers:{ 'Content-Type': 'application/json' },
        
        // parsing
        dataKey:'data', // data key, if data is property of response object, e.g. { data:..., status:...}
        resourceListKey: 'this', // list of resources - if there is no wrapper in response object, data is resource, resourceListKey:'this'
        resourceKey: 'this', // single resource data - if there is no wrapper in response object, data is resource, resourceKey:'this'
        idKey:'id', // key of id, sometimes id is represented by another key, like "_id", or "productId"
        countKey:'pagination.count', // if response contains count
        errorKey:'data', // if response status !== 200, parse errors
        
        // aditional data to map to result - will be added only if it is defined in response
        keys:{
            // 'data.max_score':'maxScore' - example result of "one" { id:..., maxScore:24 }, or "all" [{ id:... }, { id:... }].maxScore = 24 
        },
        
        // CRUD defaults
        one:{
            method:'GET',
            // idKey, dataKey, resourceKey, resourceListKey, errorKey, countKey // replace default resourceKey
            // headers:{} extends default headers
        },
        all:{ method:'GET' },
        create:{
            method:'POST',
            // TODO: implement multipart
            // multipart:{ data:{ name:'data[image][file]', isFile:true }, product_id:'product[id]' } - multipart not implemented
        },
        update:{ method:'PUT' },
        remove:{ method:'DELETE' },
        // customCommandName:{  method:'POST'  }
        
        // collection command, is set when querying datasource to use by request builders, e.g. "one", "all", "create", ...
        command:null
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
        // optimisticLock: false, // checking document version on update is enabled by default
        
        // RestDataSource options
        maxRetries: 3, // if connection fail, retry 3-times
        retryTimeout: 1000, // miliseconds between repeated requests
        hasCount: true, // if responses contains count
        autoPaging: true, // will auto request next page if query.limit not reached
        autoPagingLimit: 10000, // prevent infinite loop, if rest response does not include count, and autoPaging
        dynamicPageSize: false, // if num of results may be lower than maxLimit, before end, this will stop paging only in result is empty
        autoFetchDates: true // auto fetch dates if string match date format
    },
    cache:{
        keyPrefix:'enterprise-model-rest',
        // createKey: createCacheKey - using createKey defined in DataSource
        // duration: 3000, // duration (undefined or zero means no expiration)
        // use: false, // by default do not use cache
    }
});

/*
 * onFetch method
 */

Rest.onFetch(function(data){
    var model = this;
    if(model.constructor.getDefaults().options.autoFetchDates){
        data = object.dateStringsToDates(data);
    }
});

/*
 * Constructor methods
 */

/*
 * url encode/decode helpers
 */

Rest.addMethod('urlEncode', function(text){
    if(!text) return undefined;
    else return text.replace(/\./g, '%2E')
                .replace(/\:/g, '%3A')
                .replace(/\*/g, '%2A')
                .replace(/\(/g, '%28')
                .replace(/\)/g, '%29')
                .replace(/\#/g, '%23'); // escape "." ":" "*" "(" ")" - path template cant parse it
});

Rest.addMethod('urlDecode', function(text){
    if(!text) return undefined;
    else return text.replace(/\%2E/g, '.')
                .replace(/\%3A/g, ':')
                .replace(/\%2A/g, '*')
                .replace(/\%28/g, '(')
                .replace(/\%29/g, ')')
                .replace(/\%2f/g, '/')
                .replace(/\%2F/g, '/')
                .replace(/\%23/g, '#'); // return escaped chars to originals
});

/*
 * build request
 */

// build or customize url to use by request
Rest.addMethod('buildUrl', function(defaults, reqData){
    return (defaults.connection.endpointUrl + '/' + defaults.connection.resourceName);
});

// build or customize query object
Rest.addMethod('buildQuery', function(defaults, reqData){
    return defaults.query;
});

// build or customize body object - sometimes is usefull to modify body before send
Rest.addMethod('buildBody', function(defaults, reqData){
    return reqData;
});

// build or customize headers
Rest.addMethod('buildHeaders', function(defaults, reqData){
    return defaults.connection.headers;
});

// build or customize method - in case, when advanced rules, such as complex read queries are POSTed, but simple read is GET, etc...
Rest.addMethod('buildMethod', function(defaults, reqData){
    return (defaults.connection[ defaults.connection.command ]||{}).method || 'GET';
});

// build request (superagent) object
Rest.addMethod('buildRequest', function(defaults, reqData){
    var ModelCnst = this;
    
    var method = ModelCnst.buildMethod(defaults, reqData),
        headers = ModelCnst.buildHeaders(defaults, reqData),
        url = ModelCnst.buildUrl(defaults, reqData),
        query = ModelCnst.buildQuery(defaults, reqData),
        body = ModelCnst.buildBody(defaults, reqData);
    
    var req = request(method, url).set(headers).query(query);
    if(body) req.send(body);
    
    return req;
});

/*
 * parse response - how to parse data, before onFetch
 */

// if requested single resource
Rest.addMethod('parseResource', function(defaults, resStatus, resData, resource){
    var dataKey = (defaults.connection[ defaults.connection.command ]||{}).dataKey || defaults.connection.dataKey || '';
    var resourceKey = (defaults.connection[ defaults.connection.command ]||{}).resourceKey || defaults.connection.resourceKey;
    var idKey = (defaults.connection[ defaults.connection.command ]||{}).idKey || defaults.connection.idKey;
    
    if(resource){
        resource = object.getValue(resource, resourceKey);
    }
    else if(resData){
        var data = object.getValue(resData, dataKey);
        resource = object.getValue(data, resourceKey);
    }
    
    if(resource) resource.id = object.getValue(resource, idKey);
    return resource;
});

// if requested list of resources
Rest.addMethod('parseResourceList', function(defaults, resStatus, resData, resources){
    var ModelCnst = this;
    
    var dataKey = (defaults.connection[ defaults.connection.command ]||{}).dataKey || defaults.connection.dataKey || '';
    var resourceListKey = (defaults.connection[ defaults.connection.command ]||{}).resourceListKey || defaults.connection.resourceListKey;
    
    var data = resources || object.getValue(resData, dataKey);
    if(resourceListKey) data = object.getValue(data, resourceListKey);
    var list = [];
    
    if(Array.isArray(data)) for(var i=0;i<data.length;i++){
        list.push(ModelCnst.parseResource(defaults, resStatus, null, data[i]));
    }
    return list;
});

// if responce contains count
Rest.addMethod('parseCount', function(defaults, resStatus, resData){
    var countKey = (defaults.connection[ defaults.connection.command ]||{}).countKey || defaults.connection.countKey;
    return object.getValue(resData, countKey);
});

// if response status !== 200, then try to parse error message / object
Rest.addMethod('parseError', function(defaults, resStatus, resData){
    var errorKey = (defaults.connection[ defaults.connection.command ]||{}).errorKey || defaults.connection.errorKey;
    if(resStatus===404 || resStatus==='404') return '404: Not Found';
    else return object.getValue(resData, errorKey);
});

// if responce contains count
Rest.addMethod('parseKeys', function(defaults, resStatus, resData, parsedData){
    var keys = (defaults.connection[ defaults.connection.command ]||{}).keys || defaults.connection.keys || {};
    var value;
    
    for(var key in keys){
        value = object.getValue(resData, key);
        if(value !== undefined) object.setValue(parsedData, keys[key], value);
    }
    return parsedData;
});

/*
 * init method is for index setup, or checking data store connections, etc...
 * init should be run after any new inherited model definition
 */
Rest.addMethod('init', function(cb){
    // remove last "/" in endpointUrl
    this.getDefaults().connection.endpointUrl = this.getDefaults().connection.endpointUrl.replace(/\/$/,'');
    if(typeof cb === 'function') cb();
});

/*
 * onFetch - if data modification needed when load from data store (e.g. string to date conversion)
 */
// Rest.onFetch(function(data){ });


/*
 * Http Requests helpers
 */

// helper - repeat
Rest.addMethod('execRequest', execRequest); // store reference for use by inherited models
function execRequest(ModelCnst, parseMode, defaults, reqData, done){ // done(err, data, count)
    var maxRetries = defaults.options.maxRetries,
        retryTimeout = defaults.options.retryTimeout;
    
    eReq(defaults);
    function eReq(defaults, requestRepeats){
        var request = ModelCnst.buildRequest(defaults, reqData);
        
        request
        .end(function(err, res){
            if(err){
                requestRepeats = requestRepeats || 1;
                requestRepeats++;
                
                // if not last try, try again
                if(requestRepeats <= maxRetries) setTimeout(function(){
                    eReq(defaults, requestRepeats);
                }, retryTimeout); // try again after 1,5 sec
                
                // this is last try,
                else done(new Error('RestDataSource: Max repeated requests reached').cause(err));    
            }
            else if(!res) done(new Error('RestDataSource: No response')); // connection error
            else if(res.statusType === 5){ // server error
                done(res.error, ModelCnst.parseError(defaults, res.status, res.body));
            }
            else if(res.status === 404){ // client error
                if(['single','one','resource'].indexOf(parseMode) !== -1) done(null, null, 0);
                else if(['multiple','list','resourceList','all'].indexOf(parseMode) !== -1) done(null, [], 0);
            }
            else if(res.statusType === 4){ // client error
                done(res.error, ModelCnst.parseError(defaults, res.status, res.body));
            }
            else { // res.statusType === 1 || 2 - response ok
                var data = res.body;
                
                if(['single','one','resource'].indexOf(parseMode) !== -1){
                    data = ModelCnst.parseResource(defaults, res.status, res.body);
                }
                
                else if(['multiple','list','resourceList','all'].indexOf(parseMode) !== -1){
                    data = ModelCnst.parseResourceList(defaults, res.status, res.body);
                }
                
                var count = ModelCnst.parseCount(defaults, res.status, res.body);
                data = ModelCnst.parseKeys(defaults, res.status, res.body, data);
                done(null, data, count);
            }
        });
    }
}

// helper - read all items on multiple pages if maxLimit < query.limit
Rest.addMethod('readAll', readAll); // store reference for use by inherited models
function readAll(ModelCnst, defaults, done){ // done(err, data, count)
    var allItems = [],
        requestedLimit = defaults.options.limit,
        maxRetries = defaults.options.maxRetries,
        retryTimeout = defaults.options.retryTimeout;
        
    defaults.options.skip = defaults.options.skip || 0;
    
    readPage(defaults, allItems);
    function readPage(defaults, allItems, requestRepeats){
        var request = ModelCnst.buildRequest(defaults);
        
        request
        .end(function(err, res){
            if(err){
                requestRepeats = requestRepeats || 1;
                requestRepeats++;
                
                // if not last try, try again
                if(requestRepeats <= maxRetries) setTimeout(function(){
                    readPage(defaults, allItems, requestRepeats);
                }, retryTimeout); // try again after 1,5 sec
                
                // this is last try, 
                else done(new Error('RestDataSource: Max repeated requests reached').cause(err));
            }
            else if(!res) done(new Error('RestDataSource: No response')); // connection error
            else if(res.statusType === 5){ // server error
                done(res.error, ModelCnst.parseError(defaults, res.status, res.body));
            }
            else if(res.statusType === 4){ // client error
                done(res.error, ModelCnst.parseError(defaults, res.status, res.body));
            }
            else { // res.statusType === 1 || 2 - response ok
                var items = ModelCnst.parseResourceList(defaults, res.status, res.body);
                var count = ModelCnst.parseCount(defaults, res.status, res.body);
                var maxLimit = defaults.options.autoPagingLimit;
                var dynamicPageSize = defaults.options.dynamicPageSize;
                var readItemsCount = defaults.options.skip + items.length;
                allItems = allItems.concat(items);
                
                allItems = ModelCnst.parseKeys(defaults, res.status, res.body, items);
                
                if(!Array.isArray(items)) done(null, allItems, count); // items is not array, maybe request to single resource, or cannot get items data
                else if(!defaults.options.autoPaging) done(null, allItems, count); // auto pagign is disabled
                else if(maxLimit && !dynamicPageSize && maxLimit > items.length) done(null, allItems, count); // page size < maxLimit (there are no items left)
                else if(count === 0 || count <= readItemsCount) done(null, allItems, count); // read items = all items
                else if(requestedLimit <= readItemsCount) done(null, allItems, count); // requested limit = readItemsCount (we have enought items)
                else if(items.length === 0) done(null, allItems, count); // page size = 0 (there are no items left)
                else {
                    // autoPaging - get next page
                    defaults.options.skip = readItemsCount;
                    defaults.options.limit = maxLimit || items.length; 
                    readPage(defaults, allItems);
                }
            }
        });
    }
}

/*
 * Query builder methods - inherited from DataSource
 */

/*
 * collection().find(...).exec(...) - result is raw data, so do not fetch results
 * data source specific commands (aggregate, upsert, etc...)
 * 
 */
Rest.Collection.addMethod('exec', { cacheable:true, fetch:false }, function(command, args, cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    // set command, to use to build request
    defaults.connection.command = command;
    
    // exec request, but don't parse result
    execRequest(ModelCnst, null, defaults, args, function(err, resData, count){
        if(err && resData) cb(new Error('RestDataSource exec: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else if(err) cb(new Error('RestDataSource exec: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        else cb(null, resData);
    });
});

/*
 * collection().find(...).one(callback) - callback(err, docs) result is single fetched+filled model instance or null
 */
Rest.Collection.addMethod('one', { cacheable:true, fetch:true }, function(cb){ // cb(err, docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    // set command, to use to build request
    defaults.connection.command = 'one';
    
    // set limit to 1 resord
    defaults.options.limit = 1;
    
    // exec request, and parse result as single resource
    execRequest(ModelCnst, (defaults.query.id ? 'single' : 'list'), defaults, null, function(err, resData, count){
        if(err && resData) cb(new Error('RestDataSource one: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else if(err) cb(new Error('RestDataSource one: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        else {
            cb(null, Array.isArray(resData) ? resData[0] : resData);
        }
    });
});

/*
 * collection().find(...).all(callback) - callback(err, docs) result is array of fetched+filled model instances,
 * if nothing found returns empty array
 */
Rest.Collection.addMethod('all', { cacheable:true, fetch:true }, function(cb){ // cb(err, docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    // set command, to use to build request
    defaults.connection.command = 'all';
    
    // exec request, and parse result as resourceList
    readAll(ModelCnst, defaults, function(err, resData, count){
        if(err && resData) cb(new Error('RestDataSource all: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else if(err) cb(new Error('RestDataSource all: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        else {
            resData.count = count;
            cb(null, resData);
        }
    });
});

/*
 * collection().find(...).count(callback) - callback(err, count) result is count of documents
 */
Rest.Collection.addMethod('count', { cacheable:true, fetch:false }, function(cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    // set command, to use to build request
    defaults.connection.command = 'count';
    
    // exec request, and dont parse result, just get count
    execRequest(ModelCnst, null, defaults, null, function(err, resData, count){
        if(err && resData) cb(new Error('RestDataSource count: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else if(err) cb(new Error('RestDataSource count: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        else {
            cb(null, count);
        }
    });
});

/*
 * collection().find(...).create(data, callback) - callback(err, doc/docs) result is array
 * of created documents, if data is array, else single created document
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
// returns array of created documents if data is array, else single created document
Rest.Collection.addMethod('create', { cacheable:false, fetch:true }, function(data, cb){ // cb(err, doc/docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    var multiple = true;
    if(!Array.isArray(data)) {
        data = [ data ];
        multiple = false;
    }
    
    // set command, to use to build request
    defaults.connection.command = 'create';
    
    async.Series.each(data, function(i, next){
        var now = new Date();
        data[i].id = data[i].id || generateId(); // generate new id, if it's not set
        data[i].createdDT = now;
        data[i].modifiedDT = now;
        
        // exec request, and parse result as single resource
        execRequest(ModelCnst, 'single', defaults, data[i], function(err, resData, count){
            if(err && resData) next(new Error('RestDataSource create: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
            else if(err) next(new Error('RestDataSource create: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
            else {
                data[i] = object.extend(data[i], resData); // extend, because not all rest services returns newly created item, they just returns id, or success message
                next();
            }
        });
        
    }, function(err){
        if(err) cb(err);
        else cb(null, multiple ? data : data[0]);
    });
});

/*
 * collection().find(...).update(data, callback) - callback(err, count) result is count of updated documents
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
Rest.Collection.addMethod('update', { cacheable:false, fetch:false }, function(data, cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    data = data || {};
    
    // be carefull if bulk updating and optimisticLock, need to manual update modifiedDT
    // can't modify id
    delete data.id;
    
    // read all items
    ModelCnst.collection(defaults).all(function(err, items){
        if(err) cb(new Error('RestDataSource update: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else updateAll(items);
    });
    
    function updateAll(items){
        // set command, to use to build request
        defaults.connection.command = 'update';    
        
        // clear query, it will not be usefull in update request
        defaults.query = {};
        
        var updatedCount = 0;
        async.Series.each(items, function(i, next){
            var now = new Date();
            items[i] = object.update(items[i], data);
            items[i].modifiedDT = data.modifiedDT || now;
            
            // exec request, and dont parse result
            execRequest(ModelCnst, null, defaults, items[i], function(err, resData, count){
                if(err && resData) next(new Error('RestDataSource update: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
                else if(err) next(new Error('RestDataSource update: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
                else {
                    updatedCount++;
                    next();
                }
            });
            
        }, function(err){
            if(err) cb(err);
            else cb(null, updatedCount);
        });
    }
});

/*
 * collection().find(...).remove(callback) - callback(err, count) result is count of removed documents
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
Rest.Collection.addMethod('remove', { cacheable:false }, function(cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    // read all items
    ModelCnst.collection(defaults).all(function(err, items){
        if(err) cb(new Error('RestDataSource remove: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else removeAll(items);
    });
    
    function removeAll(items){
        // set command, to use to build request
        defaults.connection.command = 'remove';    
        
        // clear query, it will not be usefull in remove request
        defaults.query = {};
        
        var removedCount = 0;
        async.Series.each(items, function(i, next){
            
            // exec request, and dont parse result
            execRequest(ModelCnst, null, defaults, items[i], function(err, resData, count){
                if(err && resData) next(new Error('RestDataSource remove: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
                else if(err) next(new Error('RestDataSource remove: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
                else {
                    removedCount++;
                    next();
                }
            });
            
        }, function(err){
            if(err) cb(err);
            else cb(null, removedCount);
        });
    }
});


/*
 * Model instance methods - inherited from DataSource
 */