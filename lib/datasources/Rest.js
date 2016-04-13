'use strict';

var Model = require('../model.js'),
    generateId = require('nodee-utils').shortId.generate,
    object = require('nodee-utils').object,
    async = require('nodee-utils').async,
    request = require('nodee-utils').request;

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
        baseUrl:'', // 'http://api.endpoint.com/products',
        
        // default headers for every request
        headers:{ 'Content-Type': 'application/json' },
        
        // parsing
        dataKey:'data', // data key, if data is property of response object, e.g. { data:..., status:...}
        resourceListKey: 'this', // list of resources - if there is no wrapper in response object, data is resource, resourceListKey:'this'
        resourceKey: 'this', // single resource data - if there is no wrapper in response object, data is resource, resourceKey:'this'
        idKey:'id', // key of id, sometimes id is represented by another key, like "_id", or "productId"
        errorKey:'data', // if response status !== 200, parse errors
        
        countKey:'pagination.count', // if response contains count
        
        // aditional data to map to result - will be added only if it is defined in response
        additionalDataKeys:{
            // 'data.max_score':'maxScore' - example result of "one" { id:..., maxScore:24 }, or "all" [{ id:... }, { id:... }].maxScore = 24 
        },
        
        // CRUD defaults
        one:{
            url:'/{id}',
            method:'GET',
            // idKey, dataKey, resourceKey, resourceListKey, errorKey, countKey // replace default resourceKey
            // headers:{} extends default headers
        },
        all:{ url:'/', method:'GET' },
        create:{
            url:'/{id}',
            method:'POST',
            // method:'UPLOAD',
            // files:['image'] - data properties as files, they can be specified in data also { image:{ path:'mypicture.png', name:'optional file name' } }
        },
        update:{ url:'/{id}', method:'PUT' },
        remove:{ url:'/{id}', method:'DELETE' },
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
        hasCount: true, // if responses contains count
        autoPaging: true, // will auto request next page if query.limit not reached
        autoPagingLimit: 10000, // prevent infinite loop, if rest response does not include count, and autoPaging
        dynamicPageSize: false, // if num of results may be lower than maxLimit, before end, this will stop paging only in result is empty
        autoFetchDates: true, // auto fetch dates if string match date format
        simulateInlineUpdate: true, // this will read all documents that match query, than for each exec update (if false, it will perform only update)
        simulateInlineRemove: true // same as simulateInlineUpdate but for remove
    },
    cache:{
        keyPrefix:'nodee-model-rest',
        // createKey: createCacheKey - using createKey defined in DataSource
        // duration: 3000, // duration (undefined or zero means no expiration)
        // use: false, // by default do not use cache
    }
});

/*
 * onFetch - if data modification needed when load from data store (e.g. string to date conversion)
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
                .replace(/\//g, '%2F')
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

// build or customize urlTemplate
Rest.addMethod('buildTemplate', function(urlTemplate, query, reqData){
    var ModelCnst = this;
    var urlParams = (urlTemplate || '').match(/\{([^\{\}]+)/g) || [];
    var url = urlTemplate + '';
    var value;
    
    for(var i=0;i<urlParams.length;i++){
        urlParams[i] = urlParams[i].substring(1);
        value = urlParams[i] === '_command' ? defaults.connection.command : (object.deepGet(query, urlParams[i]) || (object.deepGet(reqData, urlParams[i])));
        
        url = url.replace('{' +urlParams[i]+ '}', ModelCnst.urlEncode(value));
    }
    
    if(!url) return '';
    return url[0] === '/' ? url : '/'+url;
});

// build or customize url to use by request
Rest.addMethod('buildUrl', function(defaults, reqData){
    var ModelCnst = this;
    var urlTemplate = (defaults.connection[ defaults.connection.command ]||{}).url || '';
    return (defaults.connection.baseUrl + ModelCnst.buildTemplate(urlTemplate, defaults.query, reqData));
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
    var headers = (defaults.connection[ defaults.connection.command ]||{}).headers;
    if(headers) return object.merge({}, defaults.connection.headers, headers);
    else return defaults.connection.headers;
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
    
    if(body && ['POST-MULTIPART','UPLOAD'].indexOf(req.method) > -1) {
        var fileKeys = (defaults.connection[ defaults.connection.command ]||{}).files || [];
        for(var key in body){
            if(key && body.hasOwnProperty(key)){
                if(body[key] && (fileKeys.indexOf(key) > -1 || body[key].path)){
                    req.attach(key, body[key].path || body[key], body[key].name);
                }
                else req.field(key, JSON.stringify(body[key]));
            }
        }
    }
    else if(body) req.send(body);
    
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
Rest.addMethod('parseAdditionalDataKeys', function(defaults, resStatus, resData, parsedData){
    var keys = (defaults.connection[ defaults.connection.command ]||{}).additionalDataKeys || defaults.connection.additionalDataKeys || {};
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
    // remove last "/" in baseUrl
    this.getDefaults().connection.baseUrl = this.getDefaults().connection.baseUrl.replace(/\/$/,'');
    if(typeof cb === 'function') cb();
});


/*
 * Http Requests helpers
 */

// auth before request send
Rest.addMethod('authRequest', function(ModelCnst, parseMode, defaults, reqData, done){ // done(err)
    // do something to authorize: oauth, cookie auth, etc...
    done();
});


// helper - repeat
Rest.addMethod('execRequest', execRequest); // store reference for use by inherited models
function execRequest(ModelCnst, parseMode, defaults, reqData, done){ // done(err, data, count)
    var maxRetries = defaults.options.maxRetries,
        retryTimeout = defaults.options.retryTimeout;
    
    if(ModelCnst.authRequest) ModelCnst.authRequest(ModelCnst, parseMode, defaults, reqData, function(err){
        if(err) done(new Error((ModelCnst._name||'RestDataSource')+': Authorization failed').cause(err)); 
        else eReq(defaults);
    });
    else eReq(defaults);
    
    function eReq(defaults, requestRepeats){
        var request = ModelCnst.buildRequest(defaults, reqData);
        if(['POST-MULTIPART','UPLOAD'].indexOf(request.method) > -1) request.method = 'POST';
        
        request
        .retry(maxRetries)
        .end(function(err, res){
            if(!res) done(new Error((ModelCnst._name||'RestDataSource')+': Request failed "' +request.url+ '"').cause(err));
            else if(res.statusType === 5){ // server error
                done(res.error, ModelCnst.parseError(defaults, res.status, res.body));
            }
            else if(res.status === 404){ // client error
                if(['single','one','resource'].indexOf(parseMode) !== -1) done(null, null, 0);
                else if(['multiple','list','resourceList','all'].indexOf(parseMode) !== -1) done(null, [], 0);
                else done(null, null, 0);
            }
            else if(res.statusType === 4){ // client error
                var parsedErr = ModelCnst.parseError(defaults, res.status, res.body);
                var infoErr = new Error((ModelCnst._name||'RestDataSource')+': Response status "' + res.status + '" ' + JSON.stringify(parsedErr || res.body)).cause(res.error);
                done(infoErr, parsedErr);
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
                data = ModelCnst.parseAdditionalDataKeys(defaults, res.status, res.body, data);
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
                else done(new Error((ModelCnst._name||'RestDataSource')+': Max repeated requests reached ("' +request.url+ '")').cause(err));
            }
            else if(!res) done(new Error((ModelCnst._name||'RestDataSource')+': No response ("' +request.url+ '")')); // connection error
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
                
                allItems = ModelCnst.parseAdditionalDataKeys(defaults, res.status, res.body, items);
                
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
        if(err && resData) cb(new Error((ModelCnst._name||'RestDataSource')+' exec: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else if(err) cb(new Error((ModelCnst._name||'RestDataSource')+' exec: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
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
        if(err && resData) cb(new Error((ModelCnst._name||'RestDataSource')+' one: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else if(err) cb(new Error((ModelCnst._name||'RestDataSource')+' one: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
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
        if(err && resData) cb(new Error((ModelCnst._name||'RestDataSource')+' all: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else if(err) cb(new Error((ModelCnst._name||'RestDataSource')+' all: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
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
        if(err && resData) cb(new Error((ModelCnst._name||'RestDataSource')+' count: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else if(err) cb(new Error((ModelCnst._name||'RestDataSource')+' count: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
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
    
    if(!data) return cb();
    
    var multiple = true;
    if(!Array.isArray(data)) {
        data = [ data ];
        multiple = false;
    }
    else if(data.length===0) return cb(null, []);
    
    // set command, to use to build request
    defaults.connection.command = 'create';
    
    async.Series.each(data, function(i, next){
        var now = new Date();
        data[i].id = data[i].id || generateId(); // generate new id, if it's not set
        data[i].createdDT = now;
        data[i].modifiedDT = now;
        
        // exec request, and parse result as single resource
        execRequest(ModelCnst, 'single', defaults, data[i], function(err, resData, count){
            if(err && resData) next(new Error((ModelCnst._name||'RestDataSource')+' create: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
            else if(err) next(new Error((ModelCnst._name||'RestDataSource')+' create: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
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
    if(defaults.options.simulateInlineUpdate) ModelCnst.collection(defaults).all(function(err, items){
        if(err) cb(new Error((ModelCnst._name||'RestDataSource')+' update: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else updateAll(items);
    });
    else updateAll(Array.isArray(data) ? data : [data]);
    
    function updateAll(items){
        // set command, to use to build request
        defaults.connection.command = 'update';    
        
        // clear query, it will not be usefull in update request
        if(!defaults.options.simulateInlineUpdate) defaults.query = {};
        
        var updatedCount = 0;
        async.Series.each(items, function(i, next){
            var now = new Date();
            items[i] = object.update(items[i], data);
            items[i].modifiedDT = data.modifiedDT || now;
            
            // exec request, and dont parse result
            execRequest(ModelCnst, null, defaults, items[i], function(err, resData, count){
                if(err && resData) next(new Error((ModelCnst._name||'RestDataSource')+' update: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
                else if(err) next(new Error((ModelCnst._name||'RestDataSource')+' update: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
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
    if(defaults.options.simulateInlineRemove) ModelCnst.collection(defaults).all(function(err, items){
        if(err) cb(new Error((ModelCnst._name||'RestDataSource')+' remove: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else removeAll(items);
    });
    else removeAll([{}]);
    
    function removeAll(items){
        // set command, to use to build request
        defaults.connection.command = 'remove';    
        
        // clear query, it will not be usefull in remove request
        if(!defaults.options.simulateInlineRemove) defaults.query = {};
        
        var removedCount = 0;
        async.Series.each(items, function(i, next){
            
            // exec request, and dont parse result
            execRequest(ModelCnst, null, defaults, items[i], function(err, resData, count){
                if(err && resData) next(new Error((ModelCnst._name||'RestDataSource')+' remove: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
                else if(err) next(new Error((ModelCnst._name||'RestDataSource')+' remove: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
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