'use strict';

var Model = require('../model.js'),
    object = require('nodee-utils').object;

/*
 * GoogleDataStore data source:
 */
var GoogleDataStore = module.exports = Model.define('GoogleDataStoreDataSource', [ 'RestOAuth2DataSource' ], {
    id:{ isString:true, required:true },
    createdDT:{ date:true },
    modifiedDT: { date:true },

    name:{ isString:true },
    surname:{ isString:true },
    obj:{},
    arrObj:{},
    simpleArr:{}
});

/*
 * defaults
 */
GoogleDataStore.extendDefaults({
    connection:{
        debugRequest: true, // debug requests, only for dev
        // debugResponse: true, // debug response, only for dev

        baseUrl:'https://datastore.clients6.google.com/v1beta3/projects',
        project:'',
        namespace:'',
        collection:'',

        oauth:{
            accessToken:{
                value: '', // stored token value
                expires: 0, // stored token expiration timestamp
                url: 'https://www.googleapis.com/oauth2/v4/token',
                body:[ 'grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer' ],
                username: '',
                password: '',
                tokenKey: 'access_token',
                expireKey: 'expires_in',
                jwt:{
                    iss: 'testissimo-db@testissimo-cloud.iam.gserviceaccount.com',
                    scope: 'https://www.googleapis.com/auth/datastore',
                    aud: 'https://www.googleapis.com/oauth2/v4/token',
                    exp: 30*60,
                    privateKey: '-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n',
                    type: 'RS256'
                }
            }
        },

        // parsing
        dataKey: 'this', // data key, if data is property of response object, e.g. { data:..., status:...}
        resourceListKey: 'batch.entityResults', // list of resources - if there is no wrapper in response object, data is resource, resourceListKey:'this'
        resourceKey: 'this', // single resource data - if there is no wrapper in response object, data is resource, resourceKey:'this'
        idKey: 'id', // key of id, sometimes id is represented by another key, like "_id", or "productId"
        hasNextPageKey:'moreResults',
        countKey: 'count', // if response contains count
        errorKey: 'error.message', // if response status !== 200, parse errors
        
        // additionalDataKeys:{
        //     'aggregations':'aggregations'
        // },
        
        // CRUD defaults
        one:{
            url:':runQuery',
            method:'POST',
            returnsResourceList: true,
            transformRequest: function(reqOpts, defaults){
                reqOpts.body = this.createQuery(defaults);
                reqOpts.query = {};
            }
        },
        all:{
            url:':runQuery',
            method:'POST',
            returnsResourceList: true,
            transformRequest: function(reqOpts, defaults){
                reqOpts.body = this.createQuery(defaults);
                reqOpts.query = {};
            }
        },
        create:{
            url:':commit',
            method:'POST',
            transformRequest: function(reqOpts, defaults){
                reqOpts.body = this.createCommit(defaults.options.transaction).create(reqOpts.body);
                reqOpts.query = {};
            }
        },
        update:{
            url:':commit',
            method:'POST',
            transformRequest: function(reqOpts, defaults){
                reqOpts.body.id = reqOpts.body.id || defaults.query.id;
                reqOpts.body = this.createCommit(defaults.options.transaction).update(reqOpts.body);
                reqOpts.query = {};
            }
        },
        remove:{
            url: ':commit',
            method: 'POST',
            sendBody: true,
            transformRequest: function(reqOpts, defaults){
                reqOpts.body = this.createCommit(defaults.options.transaction).remove(reqOpts.query.id ? reqOpts.query : reqOpts.body);
                reqOpts.query = {};
            }
        },
        beginTransaction: {
            url:':beginTransaction',
            method:'POST'
        },
        lookup: {
            url:':lookup',
            method:'POST'
        },
        rollback: {
            url:':rollback',
            method:'POST'
        }

    },
    options:{
        sort:{}, // createdDT:1
        limit: undefined,
        skip: 0,
        fields: {},
        softRemove: false,
        optimisticLock: true, // checking document version on update is enabled by default
        
        // RestDataSource options
        maxRetries: 3, // if connection fail, retry 3-times
        hasCount: false, // if responses contains count
        autoPaging: true, // will auto request next page if query.limit not reached
        autoPagingLimit: 5000, // prevent infinite loop, if rest response does not include count, and autoPaging
        dynamicPageSize: false, // if num of results may be lower than maxLimit, before end, this will stop paging only in result is empty
        autoFetchDates: false, // auto fetch dates if string match date format
        simulateInlineUpdate: true, // this will read all documents that match query, than for each exec update (if false, it will perform only update)
        simulateInlineRemove: true, // same as simulateInlineUpdate but for remove
        simulateOptimisticLock: false, // not needed, it is handled by transaction commit

        transaction: undefined // transaction Id, when making transactional writes
    },
    cache:{
        keyPrefix:'nodee-model-gds',
        // createKey: createCacheKey - using createKey defined in DataSource
        // duration: 3000, // duration (undefined or zero means no expiration)
        // use: false, // by default do not use cache
    }
});


/*
 * onFetch - if data modification needed when load from data store (e.g. string to date conversion)
 */

GoogleDataStore.onFetch(function(data){
    var ModelCnst = this.constructor;
    return ModelCnst.fetchGDSEntity(data);
});

GoogleDataStore.addMethod('fetchGDSEntity', function(data){
    var ModelCnst = this,
        fetchedData = {};

    if((data.entity || {}).properties){
        for(var propName in data.entity.properties){
            fetchedData[propName] = ModelCnst.parseValueProp(data.entity.properties[propName]);
        }
    }
    else return data;
    return fetchedData;
});

// if responce contains hasNextPage
GoogleDataStore.addMethod('parseHasNextPage', function(defaults, resStatus, resData){
    var hasNextPageKey = (defaults.connection[ defaults.connection.command ]||{}).hasNextPageKey || defaults.connection.hasNextPageKey;
    if(hasNextPageKey) return object.deepGet(resData, hasNextPageKey) === 'NO_MORE_RESULTS';
    else return undefined;
});

// build or customize url template to use by request
GoogleDataStore.addMethod('buildUrlTemplate', function(defaults, reqData){
    var ModelCnst = this;
    var urlTemplate = (defaults.connection[ defaults.connection.command ]||{}).url || '';
    return defaults.connection.baseUrl + '/' + defaults.connection.project + urlTemplate;
});

// build or customize entity key, not always it is equal to id, e.g. sum of two or more properties
GoogleDataStore.addMethod('buildEntityName', function(dataObj){
    return dataObj.id;
});

/*
 * Update / Remove Transaction:
 *
 * 1. create transaction
 * 2. lookup document by id
 * 3. compare modifiedDT / rollback transaction
 * 4. update / remove document
 *
 */

function rollbackTransaction(transactionId, next, opError){
    GoogleDataStore.collection().exec('rollback', {}, { transaction: transactionId }, function(err, data){
        if(err) next(new Error('GoogleDataStore transaction rollback failed').cause(err));
        else next(opError);
    });
}

function beginTransaction(next){
    var doc = this;
    GoogleDataStore.collection().exec('beginTransaction', function(err, data){
        if(err) return next(new Error('GoogleDataStore beginTransaction failed').cause(err));
        if(!data || !data.transaction) return next(new Error('GoogleDataStore transaction begin failed'));

        doc.opDefaults({ options:{ transaction:data.transaction } });
        next();
    });
}

function lookupDocument(next){
    var doc = this,
        transactionId = doc.opDefaults().options.transaction;

    var lookupBody = {
        readOptions:{
            transaction: transactionId
        },
        keys:[
            doc.constructor.createEntity(doc, true)
        ]
    };

    GoogleDataStore.collection().exec('lookup', {}, lookupBody, function(err, data){
        if(err) return rollbackTransaction(transactionId, next, new Error('GoogleDataStore lookup failed').cause(err));
        if(!data || !data.found || !data.found.length === 1) return rollbackTransaction(transactionId, next, new Error('GoogleDataStore lookup failed'));

        doc.$current = doc.constructor.fetchGDSEntity(data.found[0]);
        next();
    });
}

function compareModifiedDT(next){
    var doc = this,
        transactionId = doc.opDefaults().options.transaction;

    if(!doc.modifiedDT) {
        return rollbackTransaction(transactionId, next, new Error('GoogleDataStore transaction failed, modifiedDT is not defined').details({ code:'INVALID', validErrs:{ modifiedDT:['required'] } }));
    }
    if(doc.modifiedDT.getTime() !== doc.$current.modifiedDT.getTime()) {
        return rollbackTransaction(transactionId, next, new Error('GoogleDataStore transaction failed, document version has been changed').details({ code:'NOTFOUND' }));
    }

    next();
}

GoogleDataStore.on('beforeUpdate', 'beginTransaction', beginTransaction);
GoogleDataStore.on('beforeUpdate', 'loolupDocument', lookupDocument);
GoogleDataStore.on('beforeUpdate', 'compareModifiedDT', compareModifiedDT);

GoogleDataStore.on('beforeRemove', 'beginTransaction', beginTransaction);
GoogleDataStore.on('beforeRemove', 'loolupDocument', lookupDocument);
GoogleDataStore.on('beforeRemove', 'compareModifiedDT', compareModifiedDT);

GoogleDataStore.addMethod('createQuery', function(collectionDefaults){
    var ModelCnst = this;

    var qb = {
        partitionId: {
            namespaceId: collectionDefaults.connection.namespace || ''
        },
        // readOptions: {
        //     transaction: 
        // },

        query:{
            kind: [{ name: collectionDefaults.connection.collection }],
            
            // fields
            projection: [
                // { property:{ name:'propname' } }
            ],
            
            filter: {
                compositeFilter: {
                    op: 'AND',
                    filters: [
                        // {
                        //     propertyFilter: {
                        //         property:{ name: 'propname'},
                        //         value:{ stringValue: '123' },
                        //         op: 'EQUAL'
                        //     }
                        // }
                    ]
                }
            },
            order: [
                // {
                //     property:{ name:'propname' },
                //     direction:'ASCENDING' // DESCENDING
                // }
            ],
            offset: collectionDefaults.options.skip || 0, // skip
            // limit: collectionDefaults.options.limit,

            // distinctOn: [
            //     property:{ name: 'propname'},
            // ],
            // startCursor: string,
            // endCursor: string,
        }
    };

    var negativeFields = [];
    for(var propName in collectionDefaults.options.fields){
        if(collectionDefaults.options.fields[propName]) qb.query.projection.push({ property:{ name: propName } });
        else negativeFields.push(propName.split('.')[0]);
    }

    if(negativeFields.length > 0 && negativeFields.length === Object.keys(collectionDefaults.options.fields).length) { // only hidden fields, generate inverse projection
        qb.query.projection = [];
        var schemaFields = Object.keys(ModelCnst.getSchema());
        for(var i=0;i<schemaFields.length;i++){
            if(negativeFields.indexOf(schemaFields[i]) === -1) qb.query.projection.push({ property:{ name: schemaFields[i] } });
        }
    }
    
    if(collectionDefaults.options.limit) qb.query.limit = collectionDefaults.options.limit;

    for(var propName in collectionDefaults.options.sort){
        qb.query.order.push({
            property:{ name: propName },
            direction: collectionDefaults.options.sort[propName] === -1 ? 'DESCENDING' : 'ASCENDING'
        });
    }
    if(qb.query.order.length === 0) delete qb.query.order;

    qb.query.filter.compositeFilter.filters = ModelCnst.transformMongoFilter(collectionDefaults.query || {});
    if(qb.query.filter.compositeFilter.filters.length === 0) delete qb.query.filter;

    return qb;
});

GoogleDataStore.addMethod('transformMongoFilter', function(mongoQuery){
    var ModelCnst = this;
    var filters = [], operator;

    for(var propName in mongoQuery){
        if(propName[0] === '$') throw new Error('Google Datastore does not support operator "'+propName+'" (suppoerted is only $eq,$gt,$gte,$lt,$lte)');
        
        if(object.isPlainObject(mongoQuery[propName])){
            for(var op in mongoQuery[propName]){
                if(op === '$eq') operator = 'EQUAL';
                else if(op === '$gt') operator = 'GREATER_THAN';
                else if(op === '$gte') operator = 'GREATER_THAN_OR_EQUAL';
                else if(op === '$lt') operator = 'LESS_THAN';
                else if(op === '$lte') operator = 'LESS_THAN_OR_EQUAL';
                else throw new Error('Google Datastore does not support operator "'+op+'" (suppoerted is only $eq,$gt,$gte,$lt,$lte)');

                filters.push({
                    propertyFilter: {
                        property:{ name: propName },
                        value: ModelCnst.typeValueProp(mongoQuery[propName][op]),
                        op: op
                    }
                });
            }
        }
        else {
            filters.push({
                propertyFilter: {
                    property:{ name: propName },
                    value: ModelCnst.typeValueProp(mongoQuery[propName]),
                    op: 'EQUAL'
                }
            });
        }
    }
    
    return filters;
});

GoogleDataStore.addMethod('createCommit', function(transactionId){
    var ModelCnst = this,
        defaults = ModelCnst.getDefaults();
    
    function mutationFnc(opType, onlyKey){
        function createMutation(dataObj, onlyKey){
            var mutation = {};
            mutation[opType] = ModelCnst.createEntity(dataObj, onlyKey);
            return mutation;
        }

        return function(dataObj){
            this.mutations.push( createMutation(dataObj, onlyKey) );
            return this;
        };
    }

    return {
        mode: transactionId ? 'TRANSACTIONAL' : 'NON_TRANSACTIONAL',
        mutations: [],
        transaction: transactionId,
        create: mutationFnc('insert'),
        update: mutationFnc('update'),
        remove: mutationFnc('delete', true)
    };
});

GoogleDataStore.addMethod('createEntity', function(dataObj, onlyKey){
    var ModelCnst = this,
        defaults = ModelCnst.getDefaults();

    var key = {
        partitionId: {
            namespaceId: defaults.connection.namespace || ''
        },
        path: [{
            kind: defaults.connection.collection,
            name: ModelCnst.buildEntityName(dataObj)
        }]
    };

    if(onlyKey) return key;

    return {
        key: key,
        properties: ModelCnst.createEntityProperties(dataObj)
    };
});

GoogleDataStore.addMethod('createEntityProperties', function(dataObj, excludeFromIndexes){
    var ModelCnst = this,
        defaults = ModelCnst.getDefaults(),
        props = {};

    for(var key in dataObj){
        props[key] = ModelCnst.typeValueProp(dataObj[key]);
    }

    return props;
});

GoogleDataStore.addMethod('typeValueProp', function(value, excludeFromIndexes){
    var ModelCnst = this;

    // TODO: implement types:
    // "geoPointValue", "keyValue", "integerValue"
    var valueType = typeof value;
    var valueProp;

    if(value === null || value === undefined){
        valueProp = { nullValue: null };
    }
    else if(valueType === 'string'){
        valueProp = { stringValue:value };
        if(valueProp.stringValue.length > 100) valueProp.excludeFromIndexes = true;
    }
    else if(valueType === 'number'){
        valueProp = { doubleValue:value };
    }
    else if(valueType === 'boolean'){
        valueProp = { booleanValue:value };
    }
    else if(valueType === 'function'){
        // skip
    }
    else if(Buffer.isBuffer(value)){
        valueProp = {
            blobValue: value,
            excludeFromIndexes: true
        };
    }
    else if(object.isObject(value)){
        valueProp = { entityValue:{ properties:{} } };
        for(var k in value) {
            valueProp.entityValue.properties[k] = ModelCnst.typeValueProp(value[k], excludeFromIndexes)
        }
    }
    else if(Array.isArray(value)) {
        valueProp = { arrayValue:{ values:[] } };
        for(var i=0;i<value.length;i++) {
            valueProp.arrayValue.values[i] = ModelCnst.typeValueProp(value[i], excludeFromIndexes);
        }
    }
    else if(value instanceof Date){
        valueProp = { timestampValue: value.toISOString() };
    }
    else {
        valueProp = { stringValue:value+'' };
        if(valueProp.stringValue.length > 100) valueProp.excludeFromIndexes = true;
    }

    valueProp.excludeFromIndexes = valueProp.excludeFromIndexes || excludeFromIndexes || false;
    return valueProp;
});

GoogleDataStore.addMethod('parseValueProp', function(valueObj){

    if(valueObj.nullValue === null) return null;
    else if(valueObj.stringValue !== undefined) return valueObj.stringValue;
    else if(valueObj.doubleValue !== undefined) return valueObj.doubleValue;
    else if(valueObj.integerValue !== undefined) return parseInt(valueObj.integerValue, 10);
    else if(valueObj.booleanValue !== undefined) return valueObj.booleanValue;
    else if(valueObj.timestampValue !== undefined) return new Date(valueObj.timestampValue);
    else if(valueObj.blobValue !== undefined) return valueObj.blobValue;
    else if(valueObj.arrayValue !== undefined) {
        var result = [];
        for(var i=0;i<(valueObj.arrayValue.values || []).length;i++){
            result.push( this.parseValueProp(valueObj.arrayValue.values[i]) );
        }
        return result;
    }
    else if(valueObj.entityValue !== undefined) {
        var result = {};
        for(var propName in (valueObj.entityValue || {}).properties){
            result[propName] = this.parseValueProp(valueObj.entityValue.properties[propName]);
        }
        return result;
    }

    else throw new Error('Cannot parse, value property "'+Object.keys(valueObj)+'" not recognized');
});

/*
 * Model instance methods - inherited from DataSource
 */


// TODO: test draft, complete and move to tests
// GoogleDataStore.new({
//     id: '8',
//     name: 'jozko2',
//     surname: 'test2',
//     obj:{
//         subname:'subname',
//         subnum:123
//     },
//     arrObj:[
//         { name0:'name0', tags:['t00'] },
//         { name1:'name1', tags:['t10'] }
//     ],
//     simpleArr:[ '1','2','3', new Date(), 123 ]
// }).create(function(err, data){
//     console.warn(err, data);
// });

// GoogleDataStore.collection().remove(function(err, data){
//     console.warn(err, JSON.stringify(data));
// });

// GoogleDataStore.collection().skip(1).limit(1).all(function(err, data){
//     console.warn(err, JSON.stringify(data));
// });

// // "id":true, "createdDT", "modifiedDT", "name", "surname", "obj", "arrObj", "simpleArr"
// GoogleDataStore.collection().fields({ 'obj.subname':false, arrObj:true }).all(function(err, data){
//     console.warn(err, JSON.stringify(data));
// });

// GoogleDataStore.collection().findId('1').one(function(err, person){
//     person.name = 'updated alone';
//     //person.modifiedDT = new Date();
//     //console.warn(person);
//     person.update(function(err, person){
//         console.warn(err, person);
//     });
//     // person.remove(function(err, person){
//     //     console.warn(err, person);
//     // });
// });

// GoogleDataStore.collection().findId('1').update({ name:'updated' }, function(err, data){
//     console.warn(err, data);
// });