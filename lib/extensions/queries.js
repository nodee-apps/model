'use_strict';

var Model = require('../model.js'),
    object = require('nodee-utils').object,
    cache = require('../cache.js');


/*
 * Model "queries" extension:
 * adds query builder to constructor,
 * and inherit querybuilder methods from parent constructor
 *
 * @example: Constructor.collection().find({ ... }).all(function(err, records){ ... });
 */

Model.extensions.push({
    //instance:{},
    constructor: function(ParentCnst){
        var newConstructor = this;
        
        // add and inherit quiery builder
        var proto = new (ParentCnst.Collection || function(){})();
        if(typeof newConstructor.Collection === 'function'){
            proto = object.extend({}, new newConstructor.Collection(), proto);
        }
        newConstructor.Collection = function(){};
        newConstructor.Collection.prototype = proto;
        
        // working with query defaults
        newConstructor.Collection.prototype.extendDefaults = function(defaults){
            object.extend(true, this._defaults, defaults);
            return this;
        };
        newConstructor.Collection.prototype.setDefaults = function(defaults){
            this._defaults = object.extend(true, {}, defaults);
        };
        newConstructor.Collection.prototype.getDefaults = function(defaults){
            return this._defaults || {};
        };
        
        // adding methods to Collection
        newConstructor.Collection.addMethod = function(names, opts, fnc){
            if(arguments.length===2){
                fnc = arguments[1];
                opts = {};
            }
            opts = opts || {};
            names = Array.isArray(names) ? names : [names];
            
            var method, Collection = this;
            if(!opts.cacheable && !opts.fetch) method = fnc;
            
            // wrap callback to ensure autofetching
            else if(!opts.cacheable && opts.fetch) method = function(){
                var args = Array.prototype.slice.call(arguments, 0),
                    query = this;
                    
                // use fake callback if needed
                var cb = typeof args[ args.length-1 ] === 'function' ? args.pop() : function(err){ if(err) throw err; };
                var ModelCnst = query.getModelConstructor();
                
                args.push(function(err, data){ // query execution callback
                    if(err) cb(err); // just send error
                    else cb(null, autoFetch(true, data, ModelCnst));
                });
                return fnc.apply(query, args);
            };
            
            // wrapper around cacheable queries
            else method = function(){
                var args = Array.prototype.slice.call(arguments, 0),
                    query = this,
                    defaults = query.getDefaults();
                
                // if it has callback and need to use cache
                if((defaults.cache || {}).use === true){
                    // use fake callback if needed
                    var cb = typeof args[ args.length-1 ] === 'function' ? args.pop() : function(err){ if(err) throw err; };
                    var ModelCnst = query.getModelConstructor();
                    
                    // try get data from cache
                    var key = defaults.cache.createKey(ModelCnst._name, defaults, names[0]);
                    cache.get(key, function(err, cachedData){
                        if(err) cb(new Error('Model collection: loading from cache failed').details({ code:'CACHEFAIL', cause:err }));
                        else if(typeof cachedData !== 'undefined') { // data found in cache, need to fetch, and send back
                            cb(null, autoFetch(opts.fetch, cachedData, ModelCnst));
                        }
                        else { // data not found, call fnc and cache output
                            args.push(function(err, data){ // query execution callback
                                if(err) cb(err); // just send error
                                else {
                                    data = autoFetch(opts.fetch, data, ModelCnst);
                                    cache.put(key, getOnlyData(data), defaults.cache.duration, function(err){
                                        if(err) cb(new Error('Model collection: writing data to cache failed').details({ code:'CACHEFAIL', cause:err }));
                                        else cb(null, autoFetch(opts.fetch, data, ModelCnst));
                                    });
                                }
                            });
                            
                            return fnc.apply(query, args);
                        }
                    });
                }
                
                // cache is not used, but need to fetch, wrap callback to ensure autofetching
                else if(opts.fetch){
                    // use fake callback if needed
                    var cb = typeof args[ args.length-1 ] === 'function' ? args.pop() : function(err){ if(err) throw err; };
                    var ModelCnst = query.getModelConstructor();
                    
                    args.push(function(err, data){ // query execution callback
                        if(err) cb(err); // just send error
                        else cb(null, autoFetch(true, data, ModelCnst));
                    });
                    return fnc.apply(query, args);
                }
                
                // if fetch is false and cache is not used, return original, unwrapped callback
                else return fnc.apply(query, args);
            };
            
            for(var i=0;i<names.length;i++) {
                if(['addMethod', '_defaults', 'setDefaults', 'extendDefaults', 'getDefaults'].indexOf(names[i])!==-1)
                    throw new Error('Method name "' +names[i]+ '" is reserved');
                else
                    Collection.prototype[ names[i] ] = method;
            }
        };
        
        // quick create new Collection query
        newConstructor.collection = function(defaults){
            var c = new this.Collection();
            c._ModelConstructor = this;
            c.setDefaults(defaults || this._defaults);
            return c;
        };
        
        // helper for getting model constructor
        newConstructor.Collection.prototype.getModelConstructor = function(){
            return this._ModelConstructor;
        };
        
        return {};
    }
});

// helper for auto fetching
function autoFetch(doFetch, data, Cnst){
    if(!doFetch) return data;
    
    if(Array.isArray(data)) {
        for(var i=0;i<data.length;i++){
            data[i] = Cnst.new().fetch(data[i]);
        }
    }
    else if(data) return Cnst.new().fetch(data);
    
    return data;
}

function getOnlyData(records){
    if(Array.isArray(records)){
        for(var i=0;i<records.length;i++) records[i] = records[i].__instanceof==='Model' ? records[i].getData() : records[i];
    }
    else records = (records && records.__instanceof==='Model') ? records.getData() : records;
    
    return records;
}