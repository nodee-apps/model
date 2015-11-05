'use strict';

var Model = require('../model.js'),
    object = require('enterprise-utils').object,
    request = require('enterprise-utils').request;

/*
 * ElasticSearch data source:
 *
 * WARNING:
 * ElasticSearch has refresh_interval, so written data are not searchable immediate,
 * but after refresh_interval which is by default 1 second 
 * 
 */
var ElasticSearch = module.exports = Model.define('ElasticSearchDataSource',['RestDataSource'], {
    id:{ isString:true },
    // deleted:{ isBoolean:true }, // if softRemove
    createdDT:{ date:true },
    modifiedDT: { date:true }, // if optimisticLock
});

/*
 * defaults
 */
ElasticSearch.extendDefaults({
    connection:{
        baseUrl:'http://localhost:9200',
        index: '', // elasticsearch index
        type: '', // elasticsearch type
            
        // elasticsearch data mappings
        mapping:{
            id:{ type:'string', 'index':'not_analyzed' },
            createdDT: {
                type: 'date',
                format: 'dateOptionalTime'
            },
            updatedDT: {
                type: 'date',
                format: 'dateOptionalTime'
            }
            /* example mapping of nested schema:
             * variants:{
             *    properties:{
             *        sku:{ type:'string', 'index':'not_analyzed' },
             *        ean:{ type:'string', 'index':'not_analyzed' },
             *        internal_code:{ type:'string', 'index':'not_analyzed' }
             *    }
             * }
             */
        },
        
        // parsing
        dataKey: 'this', // data key, if data is property of response object, e.g. { data:..., status:...}
        resourceListKey: 'hits.hits', // list of resources - if there is no wrapper in response object, data is resource, resourceListKey:'this'
        resourceKey: '_source', // single resource data - if there is no wrapper in response object, data is resource, resourceKey:'this'
        idKey: 'id', // key of id, sometimes id is represented by another key, like "_id", or "productId"
        countKey: 'hits.total', // if response contains count
        errorKey: 'error', // if response status !== 200, parse errors
        
        additionalDataKeys:{
            'aggregations':'aggregations'
        },
        
        // CRUD defaults
        one:{
            method:'GET'
        },
        all:{
            method:'POST',
        },
        create:{
            resourceKey:'this',
            idKey:'_id'
        }
    },
    query:{
        // deleted:{ $ne:true } // default query when softRemove: true
    },
    options:{
        sort:{ createdDT:1 },
        limit: undefined,
        skip: 0,
        fields: {},
        softRemove: false,
        optimisticLock: true, // checking document version on update is enabled by default
        
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
        keyPrefix:'enterprise-model-elasticsearch',
        // createKey: createCacheKey - using createKey defined in DataSource
        // duration: 3000, // duration (undefined or zero means no expiration)
        // use: false, // by default do not use cache
    }
});

/*
 * collection().findId(...) - replace mongo $in operator, ElasticSearch uses terms:[ 1,2,... ]
 */
ElasticSearch.Collection.addMethod('findId', function(id){
    this.extendDefaults({query:{ id:id }}); // will be converted to term/s:{ id:value/s } by buildQuery method
    return this;
});

/*
 * Constructor methods
 */

/*
 * Building request
 */
 
/*
 * ElasticSearch search API example:
 * {
    "query": {
        "filtered": {
            "query":  { "match": { "email": "business opportunity" }},
            "filter": { "term": { "folder": "inbox" }}
        }
    }
  }
 *
 */

// replace default Rest.buildQuery method
ElasticSearch.buildQuery = function(defaults, reqData) {
    var elasticQuery = {}, filter = {};
        
    // defaults.query represents ElasticSearch filter by default, to ensure compatibility with "DataSource", e.g. optimisticLock queries, etc..
    // all query keys that not starts with "$" are simple data keys, and will be transformed to term/s:{ dataKey:value }
    for(var key in defaults.query){
        if(key[0] !== '$'){
            if(Array.isArray(defaults.query[key])){
                filter.terms = filter.terms || {};
                filter.terms[key] = defaults.query[key];
            }
            else {
                filter.term = filter.term || {};
                filter.term[key] = defaults.query[key];
            }
        }
        else elasticQuery[ key.replace(/^\$/,'') ] = defaults.query[key];
    }
    
    if(defaults.options.skip) elasticQuery.from = defaults.options.skip;
    if(defaults.options.limit) elasticQuery.size = defaults.options.limit;
    if(defaults.options.aggregate) elasticQuery.aggs = defaults.options.aggregate;
        
    if(defaults.options.sort) {
        elasticQuery.sort = [];
        var sortItem = {};
        for(var sortKey in defaults.options.sort){
            sortItem = {};
            if(defaults.options.sort[sortKey] === 1) defaults.options.sort[sortKey] = 'asc';
            if(defaults.options.sort[sortKey] === -1) defaults.options.sort[sortKey] = 'desc';
            
            sortItem[sortKey] = defaults.options.sort[sortKey];
            elasticQuery.sort.push(sortItem);
        }
    }
    
    if(defaults.options.fields) {
        elasticQuery._source = elasticQuery._source || {};
        
        for(var fieldKey in defaults.options.fields){
            if(defaults.options.fields[fieldKey] && fieldKey !== 'id'){
                elasticQuery._source.include = elasticQuery._source.include || [];
                elasticQuery._source.include.push(fieldKey);
            }
            else if(!defaults.options.fields[fieldKey] && fieldKey !== 'id') {
                elasticQuery._source.exclude = elasticQuery._source.exclude || [];
                elasticQuery._source.exclude.push(fieldKey);
            }
        }
    }
    
    // join query to filtered query if there is query and filter together
    var filtered_filter = elasticQuery.filter = object.extend(true, elasticQuery.filter, filter);
    var filtered_query = elasticQuery.query;
    
    if(object.isObject(filtered_query) &&
       object.isObject(filtered_filter) &&
       Object.keys(filtered_query).length &&
       Object.keys(filtered_filter).length){
        
        delete elasticQuery.filter;
        delete elasticQuery.query;
        
        elasticQuery.query = {
            filtered:{
                filter: filtered_filter,
                query: filtered_query
            }
        };
    }
    
    return elasticQuery;
};

// build request (superagent) object
ElasticSearch.buildRequest = function(defaults, reqData){
    var ModelCnst = this;
    
    var method = ModelCnst.buildMethod(defaults, reqData),
        headers = ModelCnst.buildHeaders(defaults, reqData),
        url = ModelCnst.buildUrl(defaults, reqData),
        query = ModelCnst.buildQuery(defaults, reqData),
        body = ModelCnst.buildBody(defaults, reqData);
    
    // if search request, or aggregate, it is better to POST query, because of ElasticSearch queries complexity 
    if(!body && (typeof defaults.query.id!=='string' || defaults.connection.command==='all')){
        method = 'POST';
        body = query;
        query = query.search_type ? { search_type: query.search_type } : {};
    }
    
    var req = request(method, url).set(headers).query(query);
    if(body) req.send(body);
    
    return req;
};

// replace default Rest.buildUrl method
ElasticSearch.buildUrl = function(defaults, reqData){
    var url = defaults.connection.baseUrl +'/'+ defaults.connection.index +'/'+ defaults.connection.type;
    
    // var command = defaults.connection[ defaults.connection.command ] || {};
    // if(command.suffixUrl) url+= command.suffixUrl; // collection.all
    
    if(typeof defaults.query.id === 'string' && defaults.connection.command!=='all') url += '/' + ElasticSearch.urlEncode(defaults.query.id); // collection one/update/... if "all", use "_search" suffix
    else if(reqData && reqData.id) url += '/' + ElasticSearch.urlEncode(reqData.id); // collection create/remove/...
    // else if(Object.keys(defaults.query).length > 0)
    else url += '/_search'; // default suffix
    
    return url;
};


/*
 * init method is for index setup, or checking data store connections, etc...
 * init should be run after any new inherited model definition
 */
ElasticSearch.addMethod('init', function(cb){
    var ModelCnst = this,
        defaults = ModelCnst.getDefaults(),
        numOfFails = 1,
        maxNumOfFails = 15, // let elastic search 30 seconds if starting on reboot, then throw Error
        checkTimeout = 2000,
        mapping,
        type = defaults.connection.type;
    
    // remove last "/" in baseUrl
    defaults.connection.baseUrl = defaults.connection.baseUrl.replace(/\/$/,'');
    var indexUrl = defaults.connection.baseUrl + '/' + defaults.connection.index;
    
    if(defaults.connection.mapping){
        mapping = {};
        mapping[type] = { properties: defaults.connection.mapping };
    }
    
    (function checkIndexes(){
        request
        .put(indexUrl)
        .end(function(err, res){
            if(!res || (res.status!==200 && res.status!==400) || ((res.body||{}).error && (res.body||{}).error.indexOf('IndexAlreadyExistsException')===-1)){
                if(numOfFails >= maxNumOfFails) throw new Error('ElasticSearchDataSource: Cannot ensure ElasticSearch index "' +indexUrl+ '" ' +JSON.stringify((err||{}).message)+ ' ' + JSON.stringify(err || (res||{}).body || ''));
                else {
                    numOfFails++;
                    setTimeout(function(){
                        checkIndexes();
                    }, checkTimeout);
                }
            }
            else if(mapping) checkMappings(mapping);
            else if(typeof cb === 'function') cb();
        });
    })();
    
    function checkMappings(mapping){
        request
        .put(indexUrl +'/'+ type +'/_mapping')
        .send(mapping)
        .end(function(err, res){
            if(!res || (res.status!==200 && res.status!==400) || ((res.body||{}).error && (res.body||{}).error.indexOf('IndexAlreadyExistsException')===-1)){
                // fail
                throw new Error('ElasticSearchDataSource: Cannot ensure ElasticSearch mappings "' +indexUrl +'/'+ type +'/_mapping'+ '" ' + JSON.stringify(err || res.body || ''));
            }
            else {
                // success
                if(typeof cb === 'function') cb();
            }
        });
    }
    
});

/*
 * onFetch - if data modification needed when load from data store (e.g. string to date conversion)
 */
//ElasticSearch.onFetch(function(data){  ...  });
