'use strict';

var Model = require('../model.js'),
    object = require('nodee-utils').object,
    request = require('nodee-utils').request;

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
        //debugRequest: true, // debug requests, only for dev
        //debugResponse: true, // debug response, only for dev
        baseUrl:'http://localhost:9200',
        index: '', // elasticsearch index
        type: '', // elasticsearch type
        
        // auth, if no username, it will not be used
        auth:{
            username:'',
            password:''
        },
        rejectUnauthorized: true, // by default, only verified certificates
            
        // elasticsearch index analysis
        settings:{
            analysis: {
                analyzer: {
                    htmlcontent: {
                        type: 'custom',
                        tokenizer: 'standard',
                        filter: ['standard', 'lowercase', 'stop', 'asciifolding'],
                        char_filter: [ 'html_strip' ]
                    },
                    content: {
                        type: 'custom',
                        tokenizer: 'standard',
                        filter: ['standard', 'lowercase', 'stop', 'asciifolding']
                    }
                }
            }
        },
        
        // elasticsearch type data mappings
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
        all:{
            method:'POST',
            url:'/_search',
            transformRequest: switchQueryAndBody
        },
        one:{
            method:'POST',
            url:'/_search',
            resourceKey:'hits.hits.0._source',
            transformRequest: switchQueryAndBody
        },
        count:{
            method:'POST',
            url:'/_count',
            countKey:'count',
            transformRequest: switchQueryAndBody
        },
        create:{
            method:'PUT',
            url:'/{id}',
            resourceKey:'this',
            idKey: '_id'
        }
    },
    query:{
        // deleted:{ $ne:true } // default query when softRemove: true
    },
    options:{
        transformMongoFilter: true, // will transform filter query from mongodb like query
        sort:{ createdDT:1 },
        limit: undefined,
        skip: 0,
        fields: {},
        softRemove: false,
        optimisticLock: true, // checking document version on update is enabled by default
        
        // RestDataSource options
        maxRetries: 3, // if connection fail, retry 3-times
        hasCount: true, // if responses contains count
        autoPaging: true, // will auto request next page if query.limit not reached
        autoPagingLimit: 10000, // prevent infinite loop, if rest response does not include count, and autoPaging
        dynamicPageSize: false, // if num of results may be lower than maxLimit, before end, this will stop paging only in result is empty
        autoFetchDates: true // auto fetch dates if string match date format
    },
    cache:{
        keyPrefix:'nodee-model-elasticsearch',
        // createKey: createCacheKey - using createKey defined in DataSource
        // duration: 3000, // duration (undefined or zero means no expiration)
        // use: false, // by default do not use cache
    }
});

function switchQueryAndBody(reqOpts){
    reqOpts.body = reqOpts.query;
    reqOpts.query = reqOpts.query.search_type ? { search_type: reqOpts.query.search_type } : {};
}

/*
 * collection().findId(...) - replace mongo $in operator, ElasticSearch uses terms:[ 1,2,... ]
 */
ElasticSearch.Collection.addMethod('findId', function(id){
    this.extendDefaults({ query:{ id:id } }); // will be converted to term/s:{ id:value/s } by buildQuery method
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

var filterOperators = {

    $and: 'must',
    $or: 'should',
    $eq: function(field, value){
        var operator = Array.isArray(value) ? 'terms' : 'term';
        var q = {};
        q[operator] = {};
        q[operator][field] = value;
        return q;
    },
    $ne: function(field, value){
        return {
            not: transformFilter(value)
        };
    },
    $in: subFormat('terms'),
    $gt: subRange('gt'),
    $gte: subRange('gte'),
    $lt: subRange('lt'),
    $lte: subRange('lte'),
    $regexp: subFormat('regexp'),
    $must: 'must',
    $must_not: 'must_not',
    $should: 'should',
    $should_not: 'should_not'

};

function subRange(operator){
    return function(field, value){
        var q = { range:{} };
        q.range[field] = {};
        q.range[field][operator] = value;
        return q;
    };
}

function subFormat(operator){
    return function(field, value){
        var q = {};
        q[operator] = {};
        q[operator][field] = value;
        return q;
    };
}

function isPrimitiveType(value){
    return  typeof value === 'string' ||
        typeof value === 'number' ||
        typeof value === 'boolean' ||
        value === null ||
        value === undefined ||
        value instanceof Date;
}

function isPrimitiveTypeArray(value){
    if(!Array.isArray(value)) return false;
    for(var i=0;i<value.length;i++){
        if(!isPrimitiveType(value[i])) return false;
    }
    return true;
}

function transformFilter(field, mongoQuery){
    if(arguments.length === 1){
        mongoQuery = arguments[0];
        field = null;
    }

    var result = [], partialResult;
    if(typeof mongoQuery === 'function') return;

    if(isPrimitiveType(mongoQuery) || isPrimitiveTypeArray(mongoQuery)){
        result.push(filterOperators.$eq(field, mongoQuery));
    }
    else for(var key in mongoQuery){
        if(typeof filterOperators[key] === 'string'){ // logical bool operator
            if(Array.isArray(mongoQuery[key])){
                partialResult = { bool:{} };
                partialResult.bool[filterOperators[key]] = mongoQuery[key];

                for(var i=0;i<partialResult.bool[filterOperators[key]].length;i++){
                    partialResult.bool[filterOperators[key]][i] = transformFilter(key, partialResult.bool[filterOperators[key]][i]);
                }
                result.push(partialResult);
            }
        }
        else if(typeof filterOperators[key] === 'function'){ // generate elastic query
            result.push(filterOperators[key](field, mongoQuery[key]));
        }
        else {
            result.push(transformFilter(key, mongoQuery[key]));
        }
    }

    result = result.length <= 1 ? (result[0] || {}) : result;
    if(Array.isArray(result)) result = { bool:{ must:result } };
    return result;
}

ElasticSearch.addMethod('transformFilter', transformFilter);

// replace default Rest.buildQuery method
ElasticSearch.addMethod('buildQuery', function(defaults, reqData) {
    var elasticQuery = {}, elasticFilter = {};
        
    // defaults.query represents ElasticSearch filter by default, to ensure compatibility with "DataSource", e.g. optimisticLock queries, etc..
    // all query keys that not starts with "$" are simple data keys, and will be transformed to term/s:{ dataKey:value }
    for(var key in defaults.query){
        if(key[0] !== '$') elasticFilter[key] = defaults.query[key];
        else elasticQuery[ key.replace(/^\$/,'') ] = defaults.query[key];
    }
    
    if(defaults.options.transformMongoFilter) elasticFilter = this.transformFilter(elasticFilter);
    
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
            if(defaults.options.fields[fieldKey]){
                elasticQuery._source.include = elasticQuery._source.include || [];
                elasticQuery._source.include.push(fieldKey);
            }
            else if(!defaults.options.fields[fieldKey]) {
                elasticQuery._source.exclude = elasticQuery._source.exclude || [];
                elasticQuery._source.exclude.push(fieldKey);
            }
        }
    }
    
    // join query to filtered query if there is query and filter together
    var filtered_filter = elasticQuery.filter = object.extend(true, elasticQuery.filter, elasticFilter);
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
});

// replace default Rest.buildUrl method
ElasticSearch.addMethod('buildUrlTemplate', function(defaults, reqData){
    var ModelCnst = this;
    var urlTemplate = defaults.connection.baseUrl +'/'+ 
                      defaults.connection.index +'/'+ 
                      defaults.connection.type +'/'+
                      ((defaults.connection[ defaults.connection.command ]||{}).url || '');
    return urlTemplate;
});

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
        if(mapping[type].properties.dynamic_templates){
            mapping[type].dynamic_templates = mapping[type].properties.dynamic_templates;
            delete mapping[type].properties.dynamic_templates;
        }
    }
    
    (function checkIndexes(){
        request
        .put(indexUrl)
        .send({ 
            settings: defaults.connection.settings || {}
        })
        .end(function(err, res){
            var errorMsg = object.deepGet(res||{}, 'body.error.reason') || 
                           object.deepGet(res||{}, 'body.error') || '';
            
            if(!res || res.status!==200 && 
               !(res.status===400 && (errorMsg.indexOf('IndexAlreadyExistsException')>-1 || errorMsg.indexOf('already exists')>-1))){
                
                if(numOfFails >= maxNumOfFails) throw new Error((ModelCnst._name||'ElasticSearchDataSource')+': Cannot ensure ElasticSearch index PUT "' +indexUrl+ '" status "' +(res||{}).status+ '" response: ' +JSON.stringify((err||{}).message)+ ' ' + JSON.stringify(err || (res||{}).body || ''));
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
            var errorMsg = object.deepGet(res||{}, 'body.error.reason') || 
                object.deepGet(res||{}, 'body.error') || '';
            
            if(!res || res.status!==200 && 
               !(res.status===400 && (errorMsg.indexOf('IndexAlreadyExistsException')>-1 || errorMsg.indexOf('already exists')>-1))){
                // fail
                throw new Error((ModelCnst._name||'ElasticSearchDataSource')+': Cannot ensure ElasticSearch mappings "' +indexUrl +'/'+ type +'/_mapping'+ '" status "' +(res||{}).status+ '" response: ' + JSON.stringify(err || (res||{}).body || ''));
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
