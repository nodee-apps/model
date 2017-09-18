'use_strict';

var Model = require('../model.js'),
    object = require('nodee-utils').object;

/*
 * Model "defaults" extension:
 * adds default settings/options object to constructor,
 * and inherit values from parent constructor
 *
 * @example: default query filters, limits
 */

Model.extensions.push({
    instance:{
        /**
         * Get / Set CRUD operation defaults - change defaults only for this instance
         * @returns {Object}  Oeration defaults
         */
        opDefaults: function(defaultsObj){
            // create hidden property _opDefaults in model instance
            if(!this._opDefaults) Object.defineProperty(this, '_opDefaults', {
                configurable:false,
                writable:true,
                enumerable:false,
                value: {}
            });

            if(defaultsObj) object.extend('data', this._opDefaults, defaultsObj);
            return this._opDefaults;
        }
    },
    constructor: function(ParentCnst){
        
        return {
            _defaults: object.extend('data', this._defaults || {}, ParentCnst._defaults || {}),
            
            extendDefaults: function(defaults){
                object.extend(true, this._defaults, defaults);
                return this;
            },
            setDefaults: function(defaults){
                this._defaults = object.extend('data', {}, defaults);
                return this;
            },
            getDefaults: function(){
                return this._defaults || {};
            }
        };
    }
});