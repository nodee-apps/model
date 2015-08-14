'use_strict';

var Model = require('../model.js'),
    object = require('enterprise-utils').object;

/*
 * Model "methods" extension:
 * helper for adding inheritable methods to constructor object,
 * and inherits from parent constructor
 *
 */

Model.extensions.push({
    //instance:{},
    constructor: function(ParentCnst){
        var obj = {
            _methods: (ParentCnst._methods || []).slice(0),
            addMethod: function(names, fnc){
                if(typeof names === 'string') names = [names];
                
                var name;
                for(var i=0;i<names.length;i++){
                    name = names[i];
                    this[name] = fnc;
                    this._methods.push(name);
                }
            }
        };
        
        for(var key in ParentCnst){
            if(obj._methods.indexOf(key) !== -1) obj[key] = ParentCnst[key];
        }
        
        return obj;
    }
});