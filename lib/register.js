'use_strict';

/*
 * Model Names Register:
 * only registered model can have relations
 *
 * Relations register:
 * there must be some evidence of relations, to prevent duplicities,
 * and easy getting relation details, such as type, or constructor
 * 
 */

var _models = {};
var _relations = {};

module.exports = {
    /**
     * Register model name and his constructor
     * @param {String} name Name of model, must be unique
     * @param {Object} modelConstructor
     */
    add: function(name, modelConstructor){
        if(_models[name]) throw new Error('Model with name "' +name+ '" already exists, choose another name.');
        _models[name] = modelConstructor;
    },
    
    /**
     * Quick check if model name is registered
     * @param {String} name model name
     * @returns {Boolean} true/false
     */
    has: function(name){
        return !!_models[name];
    },
    
    /**
     * Alias for has
     * @param {String} name model name
     * @returns {Boolean} true/false
     */
    exists: function(name){
        return !!_models[name];
    },
    
    /**
     * Model Constructor getter
     * @param {String} name registered constructor name
     * @returns {Object}  model constructor
     */
    get: function(name){
        return _models[name];
    },
    
    /**
     * Model Constructor Names getter
     * @returns {Array}  model names
     */
    getNames: function(){
        return Object.keys(_models);
    },
    
    /**
     * remove Model reference from registered model names
     * use it only when you are replacing existing Model with another
     * @param {String} name
     */
    remove: function(name){
        delete _models[name];
    },
    
    /**
     * Models relation register
     * @param {String} id unique relation id
     * @param {Object} opts relation options
     */
    setRelation: function(id, opts){
        _relations[id] = opts;
    },
    
    /**
     * Models relation getter
     * @param {String} id relation id
     * @returns {Object}  relation options
     */
    getRelation: function(id){
        return _relations[id];
    }  
};