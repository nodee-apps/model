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
        debugRequest: false, // debug requests, only for dev
        debugResponse: false, // debug responses, only for dev
        baseUrl:'', // 'http://api.endpoint.com/products',
        
        // auth, if no username, it will not be used
        auth:{
            username:'',
            password:''
        },
        // rejectUnauthorized: true, // by default, only verified certificates - deprecated by node, need to find better way to do this
        
        timeout: false, // by default there is no timeout, you can set it as number in ms
        redirects: 2, // by default there are 2 auto following redirects
        
        // default headers for every request
        headers:{ 'Content-Type': 'application/json' },
        
        // parsing
        dataKey:'data', // data key, if data is property of response object, e.g. { data:..., status:...}
        resourceListKey: 'this', // list of resources - if there is no wrapper in response object, data is resource, resourceListKey:'this'
        resourceKey: 'this', // single resource data - if there is no wrapper in response object, data is resource, resourceKey:'this'
        idKey:'id', // key of id, sometimes id is represented by another key, like "_id", or "productId"
        errorKey:'data', // if response status !== 200, parse errors
        
        countKey:'pagination.count', // if response contains count
        hasNexPageKey:'', // if response contains next page indicator
        
        // aditional data to map to result - will be added only if it is defined in response
        additionalDataKeys:{
            // 'data.max_score':'maxScore' - example result of "one" { id:..., maxScore:24 }, or "all" [{ id:... }, { id:... }].maxScore = 24 
        },
        
        // by default it expects that all commands will return resource, this can be changed at command level
        returnsResource: true,
        
        // CRUD defaults
        one:{
            url:'/{id}',
            method:'GET',
            // transformRequest: function(reqOpts){ reqOpts.method, headers, url, query, body... },
            // idKey, dataKey, resourceKey, resourceListKey, errorKey, countKey // replace default resourceKey
            // headers:{} extends default headers
        },
        all:{ 
            url:'/', 
            method:'GET',
            returnsResourceList: true
        },
        create:{
            url:'/{id}',
            method:'POST',
            // method:'UPLOAD',
            // files:['image'] - data properties as files, they can be specified in data also { image:{ path:'mypicture.png', name:'optional file name' } }
        },
        update:{ url:'/{id}', method:'PUT' },
        remove:{ url:'/{id}', method:'DELETE', returnsResource:false, sendBody:false }, // sendBody:true by default
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
        autoPagingLimit: 5000, // prevent infinite loop, if rest response does not include count, and autoPaging
        dynamicPageSize: false, // if num of results may be lower than maxLimit, before end, this will stop paging only in result is empty
        autoFetchDates: true, // auto fetch dates if string match date format
        simulateInlineUpdate: true, // this will read all documents that match query, than for each exec update (if false, it will perform only update)
        simulateInlineRemove: true, // same as simulateInlineUpdate but for remove
        simulateOptimisticLock: false // will get old version and compare modifiedDT before update or remove
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
    if(this.constructor.getDefaults().options.autoFetchDates){
        return object.dateStringsToDates(data);
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

// build or customize url template to use by request
Rest.addMethod('buildUrlTemplate', function(defaults, reqData){
    var ModelCnst = this;
    var urlTemplate = (defaults.connection[ defaults.connection.command ]||{}).url || '';
    return defaults.connection.baseUrl + '/' + urlTemplate;
});

// build or customize url
Rest.addMethod('buildUrl', function(urlTemplate, defaults, reqData){
    var ModelCnst = this;
    var urlParams = (urlTemplate || '').match(/\{([^\{\}]+)/g) || [];
    var url = urlTemplate + '';
    var query = defaults.query;
    var value, bodyDeepGet;
    
    for(var i=0;i<urlParams.length;i++){
        urlParams[i] = urlParams[i].substring(1);
        bodyDeepGet = object.deepGet(reqData, urlParams[i]);
        value = urlParams[i] === '_command' ? defaults.connection.command : (bodyDeepGet !== undefined ? bodyDeepGet : object.deepGet(query, urlParams[i]));
        url = url.replace('{' +urlParams[i]+ '}', ModelCnst.urlEncode(value+''));
    }
    
    if(!url) return '';
    return url.replace(/([^:])(\/){2,}/g,'$1/');
});

// build or customize query object
Rest.addMethod('buildQuery', function(defaults, reqData){
    return defaults.query;
});

// build or customize body object - sometimes is usefull to modify body before send
Rest.addMethod('buildBody', function(defaults, reqData){
    var cmdConf = (defaults.connection[ defaults.connection.command ]||{});
    return cmdConf.sendBody === false ? {} : reqData;
});

// build or customize headers
Rest.addMethod('buildHeaders', function(defaults, reqData){
    var headers = (defaults.connection[ defaults.connection.command ]||{}).headers;
    if(typeof headers === 'function') return headers(defaults, reqData);
    else if(headers) return object.merge({}, defaults.connection.headers, headers);
    else return defaults.connection.headers;
});

// build or customize method - in case, when advanced rules, such as complex read queries are POSTed, but simple read is GET, etc...
Rest.addMethod('buildMethod', function(defaults, reqData){
    return (defaults.connection[ defaults.connection.command ]||{}).method || 'GET';
});

// build request (superagent) object
Rest.addMethod('buildRequest', function(defaults, reqData){
    var ModelCnst = this;

    var reqOpts = {
        method: ModelCnst.buildMethod(defaults, reqData), 
        headers: ModelCnst.buildHeaders(defaults, reqData),
        url: ModelCnst.buildUrl( ModelCnst.buildUrlTemplate(defaults, reqData) , defaults, reqData), 
        query: ModelCnst.buildQuery(defaults, reqData), 
        body: ModelCnst.buildBody(defaults, reqData)
    };

    var transformRequest = (defaults.connection[ defaults.connection.command ]||{}).transformRequest || 
                           defaults.connection.transformRequest;
    
    if(typeof transformRequest === 'function') transformRequest.call(ModelCnst, reqOpts, defaults);
    
    if(defaults.connection.debugRequest === true) {
        console.warn('\n'+(ModelCnst._name||'RestDataSource')+' request: ' + reqOpts.method +' '+ reqOpts.url);
        console.warn((ModelCnst._name||'RestDataSource')+' request headers: ' + JSON.stringify(reqOpts.headers, null, 4));
        console.warn((ModelCnst._name||'RestDataSource')+' request query: ' + JSON.stringify(reqOpts.query, null, 4));
        console.warn((ModelCnst._name||'RestDataSource')+' request body: ' + JSON.stringify(reqOpts.body, null, 4));
    }
    
    var req = request(reqOpts.method, reqOpts.url)
                .timeout(defaults.connection.timeout)
                .redirects(defaults.connection.redirects)
                .set(reqOpts.headers)
                .query(reqOpts.query);
    
    if(defaults.connection.auth && defaults.connection.auth.username) {
        req.auth(defaults.connection.auth.username, defaults.connection.auth.password);
    }
    
    if(reqOpts.body){
        if(['POST-MULTIPART','UPLOAD'].indexOf(req.method) > -1) {
            var fileKeys = (defaults.connection[ defaults.connection.command ]||{}).files || [];
            for(var key in reqOpts.body){
                if(key && reqOpts.body.hasOwnProperty(key)){
                    if(reqOpts.body[key] && (fileKeys.indexOf(key) > -1 || reqOpts.body[key].path)){
                        req.attach(key, reqOpts.body[key].path || reqOpts.body[key], reqOpts.body[key].name);
                    }
                    else req.field(key, JSON.stringify(reqOpts.body[key]));
                }
            }
            request.method = 'POST';
        }
        else req.send(reqOpts.body);
    }
    
    if(typeof defaults.connection.debugRequest === 'function') defaults.connection.debugRequest(req, reqOpts);
    
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
        resource = object.deepGet(resource, resourceKey);
    }
    else if(resData){
        var data = object.deepGet(resData, dataKey);
        resource = object.deepGet(data, resourceKey);
    }
    
    if(resource && object.deepHasProperty(resource, idKey)) resource.id = object.deepGet(resource, idKey);
    return resource;
});

// if requested list of resources
Rest.addMethod('parseResourceList', function(defaults, resStatus, resData, resources){
    var ModelCnst = this;
    
    var dataKey = (defaults.connection[ defaults.connection.command ]||{}).dataKey || defaults.connection.dataKey || '';
    var resourceListKey = (defaults.connection[ defaults.connection.command ]||{}).resourceListKey || defaults.connection.resourceListKey;
    
    var data = resources || object.deepGet(resData, dataKey);
    if(resourceListKey) data = object.deepGet(data, resourceListKey);
    var list = [];
    
    if(Array.isArray(data)) for(var i=0;i<data.length;i++){
        list.push(ModelCnst.parseResource(defaults, resStatus, null, data[i]));
    }
    return list;
});

// if responce contains count
Rest.addMethod('parseCount', function(defaults, resStatus, resData){
    var countKey = (defaults.connection[ defaults.connection.command ]||{}).countKey || defaults.connection.countKey;
    return object.deepGet(resData, countKey);
});

// if responce contains hasNextPage
Rest.addMethod('parseHasNextPage', function(defaults, resStatus, resData){
    var hasNextPageKey = (defaults.connection[ defaults.connection.command ]||{}).hasNextPageKey || defaults.connection.hasNextPageKey;
    if(hasNextPageKey) return object.deepGet(resData, hasNextPageKey);
    else return undefined;
});

// if response status !== 200, then try to parse error message / object
Rest.addMethod('parseError', function(defaults, resStatus, resData){
    var errorKey = (defaults.connection[ defaults.connection.command ]||{}).errorKey || defaults.connection.errorKey;
    if(resStatus===404 || resStatus==='404') return '404: Not Found';
    else return object.deepGet(resData, errorKey);
});

// if responce contains count
Rest.addMethod('parseAdditionalDataKeys', function(defaults, resStatus, resData, parsedData){
    if(!parsedData) return parsedData;
    var keys = (defaults.connection[ defaults.connection.command ]||{}).additionalDataKeys || defaults.connection.additionalDataKeys || {};
    var value;
    
    for(var key in keys){
        value = object.deepGet(resData, key);
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
    if(this.getDefaults().connection.baseUrl) this.getDefaults().connection.baseUrl = this.getDefaults().connection.baseUrl.replace(/\/$/,'');
    if(typeof cb === 'function') cb();
});


/*
 * Http Request Interceptors
 */

Rest.addMethod('addRequestInterceptor', function(fnc){
    var ModelCnst = this;

    if(typeof arguments[0] !== 'function') throw new Error('Wrong arguments');
    ModelCnst.reqInterceptors = ModelCnst.reqInterceptors || [];
    ModelCnst.reqInterceptors.push(fnc);
});

Rest.addMethod('getRequestInterceptors', function(){
    var ModelCnst = this,
        reqInterceptors = [];

    for(var i=1;i<ModelCnst.__parents.length;i++){
        reqInterceptors = reqInterceptors.concat(Model(ModelCnst.__parents[i]).reqInterceptors || []);
    }

    return reqInterceptors.concat(ModelCnst.reqInterceptors || []);
});

Rest.addMethod('runRequestInterceptors', function(ModelCnst, defaults, request, done){ // done(err)
    var ModelCnst = this,
        reqInterceptors = ModelCnst.getRequestInterceptors();

    async.Series.each(reqInterceptors, function(i, next){
        reqInterceptors[i].call(ModelCnst, defaults, request, next);
    }, done);
});

/*
 * Http Response Interceptors
 */

Rest.addMethod('addResponseInterceptor', function(fnc){
    var ModelCnst = this;

    if(typeof arguments[0] !== 'function') throw new Error('Wrong arguments');
    ModelCnst.resInterceptors = ModelCnst.resInterceptors || [];
    ModelCnst.resInterceptors.push(fnc);
});

Rest.addMethod('getResponseInterceptors', function(){
    var ModelCnst = this,
        resInterceptors = [];

    for(var i=1;i<ModelCnst.__parents.length;i++){
        resInterceptors = resInterceptors.concat(Model(ModelCnst.__parents[i]).resInterceptors || []);
    }

    return resInterceptors.concat(ModelCnst.resInterceptors || []);
});

Rest.addMethod('runResponseInterceptors', function(ModelCnst, defaults, repeatsCount, err, res, done){ // done(err, repeat)
    var ModelCnst = this,
        doRepeat = false,
        resInterceptors = ModelCnst.getResponseInterceptors();

    async.Series.each(resInterceptors, function(i, next){
        resInterceptors[i].call(ModelCnst, defaults, repeatsCount, err, res, function(err, repeat){
            doRepeat = doRepeat || repeat;
            next(err);
        });
    }, function(err){
        done(err, doRepeat);
    });
});

/*
 * Http Requests Helpers
 */

Rest.addMethod('execRequest', execRequest); // store reference for use by inherited models
function execRequest(ModelCnst, defaults, reqData, done){ // done(err, data, count, rawResponse)
    if(arguments.length === 3){
        done = arguments[2];
        reqData = null;
    }
    
    var allItems = [],
        repeatsCount = 0,
        requestedLimit = defaults.options.limit,
        prevPageSize = 0,
        isList = (defaults.connection[ defaults.connection.command ]||{}).isList || (defaults.connection[ defaults.connection.command ]||{}).returnsResourceList,
        returnsResource = (defaults.connection[ defaults.connection.command ]||{}).returnsResource,
        maxRetries = defaults.options.maxRetries;

    if(returnsResource === undefined) returnsResource = defaults.connection.returnsResource;
    
    defaults.options.skip = defaults.options.skip || 0;
    
    eReq(defaults);
    
    function eReq(defaults){
        var request = ModelCnst.buildRequest(defaults, reqData);

        if(ModelCnst.runRequestInterceptors) ModelCnst.runRequestInterceptors(ModelCnst, defaults, request, function(err){
            if(err) done(new Error((ModelCnst._name||'RestDataSource')+': Request interceptor failed').cause(err)); 
            else runRequest(request, defaults);
        });
        else runRequest(request, defaults);
    }

    function runRequest(request, defaults){
        request
        .retry(maxRetries)
        .end(function(err, res){
            if(ModelCnst.runResponseInterceptors) ModelCnst.runResponseInterceptors(ModelCnst, defaults, repeatsCount, err, res, function(err, repeat){
                if(err) done(new Error((ModelCnst._name||'RestDataSource')+': Response Interceptor failed').cause(err));
                else if(repeat) {
                    repeatsCount++;
                    eReq(defaults);
                }
                else handleResponse(err, request, res);
            });
            else handleResponse(err, request, res);
        });
    }

    function handleResponse(err, request, res){
        if(typeof defaults.connection.debugResponse === 'function') defaults.connection.debugResponse(res);
        else if(defaults.connection.debugResponse === true) {
            console.warn('\n'+(ModelCnst._name||'RestDataSource')+' response for: ' + request.method +' '+ request.url);
            if(!res) console.warn('No Response');
            else {
                console.warn((ModelCnst._name||'RestDataSource')+' response statusCode: ' + res.status);
                console.warn((ModelCnst._name||'RestDataSource')+' response headers: ' + JSON.stringify(res.headers, null, 4));
                console.warn((ModelCnst._name||'RestDataSource')+' response body: ' + JSON.stringify(res.body || res.text, null, 4));
            }
        }
        
        if(!res && err) done(new Error((ModelCnst._name||'RestDataSource')+': Request failed "' +request.url+ '"').details({ code:'CONNFAIL', cause:err }));
        else if(!res && !err) done(new Error((ModelCnst._name||'RestDataSource')+': Request failed "' +request.url+ '"').details({ code:'CONNFAIL' }));
        
        
        else if(res.statusType === 5){ // server error
            var parsedErr = ModelCnst.parseError(defaults, res.status, res.body);
            done(new Error((ModelCnst._name||'RestDataSource')+': Response status "'+res.status+'" ' + JSON.stringify(parsedErr || res.body)).details({ code:'EXECFAIL', cause:res.error }), parsedErr, null, res);
        }
        else if(res.status === 404){ // client error
            if(!isList) done(null, null, 0, res);
            else if(isList) done(null, [], 0, res);
            else done(null, null, 0, res);
        }
        else if(res.statusType === 4){ // client error
            var parsedErr = ModelCnst.parseError(defaults, res.status, res.body);
            var infoErr = new Error((ModelCnst._name||'RestDataSource')+': Response status "' + res.status + '" ' + JSON.stringify(parsedErr || res.body)).details({ code:'EXECFAIL', cause: res.error });
            done(infoErr, parsedErr, null, res);
        }
        else if(isList) { // res.statusType === 1 || 2 - response ok
            var items = ModelCnst.parseResourceList(defaults, res.status, res.body);
            var count = ModelCnst.parseCount(defaults, res.status, res.body);
            var hasNextPage = ModelCnst.parseHasNextPage(defaults, res.status, res.body);

            var maxLimit = defaults.options.autoPagingLimit;
            var dynamicPageSize = defaults.options.dynamicPageSize;
            var readItemsCount = defaults.options.skip + items.length;
            allItems = allItems.concat(items);
            allItems = ModelCnst.parseAdditionalDataKeys(defaults, res.status, res.body, allItems);

            if(!Array.isArray(items)) done(null, allItems, count, res); // items is not array, maybe request to single resource, or cannot get items data
            else if(!defaults.options.autoPaging) done(null, allItems, count, res); // auto pagign is disabled
            else if(!dynamicPageSize && maxLimit && allItems.length >= maxLimit) done(null, allItems, count, res); // all items count >= maxLimit (we have enought items)
            else if(!dynamicPageSize && prevPageSize && prevPageSize > items.length) done(null, allItems, count, res); // page size < prev. page size ,page size is constant (there are no items left)
            else if(count === 0 || count <= readItemsCount) done(null, allItems, count, res); // read items = all items
            else if(requestedLimit && requestedLimit <= readItemsCount) done(null, allItems, count, res); // requested limit = readItemsCount (we have enought items)
            else if(items.length === 0) done(null, allItems, count, res); // page size = 0 (there are no items left)
            else if(hasNextPage === false) done(null, allItems, count, res); // has not next page
            else {
                // autoPaging - get next page
                prevPageSize = items.length;
                defaults.options.skip = readItemsCount;
                defaults.options.limit = maxLimit || items.length;
                eReq(defaults);
            }
        }
        else { // res.statusType === 1 || 2 - response ok
            var data = returnsResource ? ModelCnst.parseResource(defaults, res.status, res.body) : res.body;
            var count = ModelCnst.parseCount(defaults, res.status, res.body);
            data = ModelCnst.parseAdditionalDataKeys(defaults, res.status, res.body, data);
            done(null, data, count, res);
        }
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
Rest.Collection.addMethod('exec', { cacheable:true, fetch:false }, function(command, reqQuery, reqData, cb){ // cb(err, data, count, res)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    if(arguments.length === 3){
        cb = arguments[2];
        reqData = arguments[1];
        reqQuery = {};
    }
    else if(arguments.length === 2){
        cb = arguments[1];
        reqData = {};
        reqQuery = {};
    }
    
    if(typeof cb !== 'function') throw new Error('Wrong arguments');
    defaults.query = reqQuery;
    
    // set command, to use to build request
    defaults.connection.command = command;
    
    // exec request, but don't parse result
    execRequest(ModelCnst, defaults, reqData, function(err, resData, count, res){
        if(err && resData) cb(new Error((ModelCnst._name||'RestDataSource')+' exec: EXECFAIL').details({ code:'EXECFAIL', cause:err }), resData, count, res);
        else if(err) cb(new Error((ModelCnst._name||'RestDataSource')+' exec: CONNFAIL').details({ code:'CONNFAIL', cause:err }), resData, count, res);
        else cb(null, resData, count, res);
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
    execRequest(ModelCnst, defaults, function(err, resData, count, res){
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
    execRequest(ModelCnst, defaults, function(err, resData, count, res){
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
    execRequest(ModelCnst, defaults, function(err, resData, count, res){
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
        data[i].createdDT = data[i].createdDT || now;
        data[i].modifiedDT = data[i].modifiedDT || data[i].createdDT;
        
        // exec request, and parse result as single resource
        execRequest(ModelCnst, defaults, data[i], function(err, resData, count, res){
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
    // can't modify id if bulk op
    if(!defaults.singleInstanceOp) delete data.id;

    // read all items
    if(!defaults.singleInstanceOp && defaults.options.simulateInlineUpdate) ModelCnst.collection(defaults).all(function(err, items){
        if(err) cb(new Error((ModelCnst._name||'RestDataSource')+' update: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else updateAll(items);
    });
    else if(defaults.singleInstanceOp && defaults.options.simulateOptimisticLock) ModelCnst.collection(defaults).one(function(err, item){
        if(err) cb(new Error((ModelCnst._name||'RestDataSource')+' update: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else if(!item || !equalDates(item.modifiedDT, defaults.query.modifiedDT)) cb(null, 0);
        else updateAll(Array.isArray(data) ? data : [data]);
    });
    else updateAll(Array.isArray(data) ? data : [data]);
    
    function updateAll(items){
        // set command, to use to build request
        defaults.connection.command = 'update';
        
        var updatedCount = 0;
        async.Series.each(items, function(i, next){
            var now = new Date();
            items[i] = object.update(items[i], data);
            items[i].modifiedDT = data.modifiedDT || now;

            // exec request, and dont parse result
            execRequest(ModelCnst, defaults, items[i], function(err, resData, count, res){
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
    if(!defaults.singleInstanceOp && defaults.options.simulateInlineRemove) ModelCnst.collection(defaults).all(function(err, items){
        if(err) cb(new Error((ModelCnst._name||'RestDataSource')+' remove: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else removeAll(items);
    });
    else if(defaults.singleInstanceOp && defaults.options.simulateOptimisticLock) ModelCnst.collection(defaults).one(function(err, item){
        if(err) cb(new Error((ModelCnst._name||'RestDataSource')+' remove: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else if(!item || !equalDates(item.modifiedDT, defaults.query.modifiedDT)) cb(null, 0);
        else removeAll([{}]);
    });
    else removeAll([{}]);
    
    function removeAll(items){
        // set command, to use to build request
        defaults.connection.command = 'remove';
        
        var removedCount = 0;
        async.Series.each(items, function(i, next){

            // exec request, and dont parse result
            execRequest(ModelCnst, defaults, items[i], function(err, resData, count, res){
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

 /*
  * helpers
  */

function equalDates(d1, d2){
    if(d1 instanceof Date && d2 instanceof Date){
        return d1.getTime() === d2.getTime();
    }
}