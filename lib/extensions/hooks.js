'use_strict';

var Model = require('../model.js'),
    object = require('enterprise-utils').object,
    async = require('enterprise-utils').async;

/*
 * Model "hooks" extension:
 * adds async hooks builder to constructor,
 * and inherit registered hook listeners from parent constructor
 *
 * @example: Constructor.on('beforeCreate', fnc);
 */

Model.extensions.push({
    //instance:{},
    constructor: function(ParentCnst){
        var newConstructor = this;
        
        // add async EE behaviour to object
        eeBehaviour(newConstructor);
        
        // copy all listener from parent
        newConstructor._listeners = newConstructor._listeners || {};
        for(var event in (ParentCnst.listeners ? ParentCnst.listeners() : {})){
            
            // copy all listeners from parent
            newConstructor._listeners[event] = (newConstructor._listeners[event] || []).concat(ParentCnst.listeners()[event] || []);
        }
        
        // helper for creating async, inheritable hooks
        newConstructor.wrapHooks = function(methodName, method){
            methodName = methodName.charAt(0).toUpperCase() + methodName.slice(1);
            return hooksWrapper('before' + methodName,'after' + methodName, 'constructor', method);
        };
        
        return {};
    }
});


/**
 * adds "series" event emmiter behaviour to object
 * @param {Object} obj object to decorate
 */
function eeBehaviour(obj){
    
    /**
     * listeners object 'eventName':[ fnc1, fnc2, ... ]
     * @type {Object}
     */
    obj._listeners = obj._listeners || {};
    
    /**
     * Listeners getter
     * @param {String} eventName event name (optional)
     * @param {String} listName listener name (optional)
     * @returns {Object}  Array, or single listener
     */
    obj.listeners = function(eventName, listName){
        if(eventName) {
            if(listName) {
                for(var i=0;i<(this._listeners[eventName] || []).length;i++){
                    if(this._listeners[eventName][i]._listenerName === listName)
                        return this._listeners[eventName][i]; 
                }
            }
            else return this._listeners[eventName] || [];
        }
        else return this._listeners;
    };
    
    /**
     * register listener to ee
     * @param {String} eventName
     * @param {Object} opts (optional) { name: 'listenerName', addBefore/addAfter: 'anotherListenerName' }
     * @param {Function} fnc listener function
     */
    obj.addListener = obj.on = function(eventName, opts, fnc){
        if(arguments.length===2 && typeof arguments[1] === 'function'){
            fnc = arguments[1];
            opts = {};
        }
        
        if(typeof fnc !== 'function') throw new Error('Listener to event "' +eventName+ '" have to be function');
        
        opts = (typeof opts === 'string') ? { name:opts } : {};
        this._listeners[eventName] = this._listeners[eventName] || [];
        
        if(opts.name){
            if(this.listeners(eventName, opts.name))
                throw new Error('Event "' +eventName+ '" already has listener with name "' +opts.name+ '"');
            else fnc._listenerName = opts.name;
        }
        
        if(opts.addBefore) {
            var addIndex;
            if(typeof opts.addBefore === 'function') addIndex = this._listeners[eventName].indexOf(opts.addBefore);
            else addIndex = this._listeners[eventName].indexOf(this.listeners(eventName, opts.addBefore));
            
            if(addIndex >=0 ) this._listeners[eventName].splice(addIndex, 0, fnc);
            else throw new Error('Can\'t add listener "' +opts.name+ '" addBefore index is "-1"');
        }
        else if(opts.addAfter) {
            var addIndex;
            if(typeof opts.addAfter === 'function') addIndex = this._listeners[eventName].indexOf(opts.addAfter);
            else addIndex = this._listeners[eventName].indexOf(this.listeners(eventName, opts.addAfter));
            
            if(addIndex >=0 ) this._listeners[eventName].splice(addIndex+1, 0, fnc);
            else throw new Error('Can\'t add listener "' +opts.name+ '" addAfter index is "-1"');
        }
        else this._listeners[eventName].push(fnc);
    };
    
    /**
     * Sometimes is usefull to replace existing listener function with another - extending business logic
     * @param {String} eventName even name
     * @param {String} listener listener name
     * @param {Function} fnc listener function
     */
    obj.replaceListener = obj.overrideListener = function(eventName, listener, fnc){
        if(typeof listener === 'string') {
            fnc._listenerName = listener;
            listener = this.listeners(eventName, listener);
        }
        var index = this.listeners[eventName].indexOf(listener);
        if(index!==-1){
            this._listeners[eventName].splice(index, 1, fnc);
        }
        else throw new Error('Can\'t override, listener not found');
    };
    
    /**
     * Remove named listener
     * @param {String} eventName even name
     * @param {String} listener listener name
     */
    obj.removeListener = function(eventName, listener){
        if(typeof listener === 'string') listener = this.listeners(eventName, listener);
        var index = this.listeners[eventName].indexOf(listener);
        if(index!==-1){
            this._listeners[eventName].splice(index,1);
        }
    };
    
    /**
     * emit event - will run all listeners in series order
     * @param {String} eventName event name
     * @param {Function} callback possible error
     */
    obj.emit = function(eventName, callback){ // last argument is callback
        var ee = this;
        var args = [];
        var hasCB = false;
        
        if(arguments.length > 1){
            hasCB = typeof arguments[arguments.length-1]==='function';
            for(var i=1;i<arguments.length-(hasCB ? 1 : 0);i++){
                args.push(arguments[i]);
            }
        }
        async.Series.each(ee.listeners(eventName), function(i, next){
            var sArgs = args.slice(0);
            sArgs.push(next);
            ee.listeners(eventName)[i].apply(ee, sArgs);
        }, callback);
    };
    
    // TODO: implement
    obj.setMaxListeners = obj.once = obj.removeAllListeners = function(){
        throw new Error('Not implemented');    
    };
}

/**
 * wrap async series hooks around method
 * @param {String} before name of before event
 * @param {String} after name of after event
 * @param {Object} eeOrPropName ee object or property name
 * @param {Function} fnc method wich will be wrapped
 * @returns {Function}  wrapped method
 */
function hooksWrapper(before, after, eeOrPropName, fnc){
    // use propName (eeOrPropName) to get ee whlie executing, this is crucial for Model inheritance
    
    return function(){
        var thisObj = this;
        var callback = arguments[arguments.length-1];
        if(typeof callback !== 'function') throw new Error('Callback is required');
        
        var args = [];
        if(arguments.length > 1){
            for(var i=0;i<arguments.length-1;i++){
                args.push(arguments[i]);
            }
        }
        
        var cbArgs = [];
        var thisEE = eeOrPropName;
        if(typeof eeOrPropName === 'string') thisEE = this[eeOrPropName];        
        var beforeListeners = thisEE.listeners(before) || [];
        var afterListeners = thisEE.listeners(after) || [];
        
        function beforeFnc(){
            async.Series.each(beforeListeners, function(i, next){
                var sArgs = args.slice(0);
                sArgs.push(next);
                beforeListeners[i].apply(thisObj, sArgs);
            },
            function(err){
                if(err) done(arguments);
                else runFnc();
            });
        }
        
        function runFnc(){
            var sArgs = args.slice(0);
            // simulate callback
            sArgs.push(function(err){
                cbArgs = Array.prototype.slice.call(arguments);
                afterFnc(err);
            });
            fnc.apply(thisObj, sArgs);
        }
        
        function afterFnc(err){
            if(err) done(cbArgs);
            else async.Series.each(afterListeners, function(i, next){
                // use call instead of apply, because cbArgs.length can be different (usually [err],[data])
                afterListeners[i].call(thisObj, cbArgs, next);
            },
            function(err){
                if(err) done(arguments);
                else done(cbArgs);
            });
        }
        
        function done(resultArgs){
            resultArgs = Array.prototype.slice.call(resultArgs);
            callback.apply(thisObj, resultArgs);
        }
        
        // run intercepted functions
        beforeFnc();
    };
}
