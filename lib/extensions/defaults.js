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
    //instance:{},
    constructor: function(ParentCnst){
        
        return {
            _defaults: object.extend(true, this._defaults || {}, ParentCnst._defaults || {}),
            
            extendDefaults: function(defaults){
                object.extend(true, this._defaults, defaults);
                return this;
            },
            setDefaults: function(defaults){
                this._defaults = object.extend(true, {}, defaults);
                return this;
            },
            getDefaults: function(){
                return this._defaults || {};
            }
        };
    }
});