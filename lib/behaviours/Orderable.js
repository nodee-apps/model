'use strict';

var Model = require('../model.js'),
    async = require('enterprise-utils').async;


var Orderable = module.exports = Model.define('Orderable', {
    sortOrder:{ isInteger:true } // uses Date().getTime, but can be anything orderable
});

Orderable.extendDefaults({
    options:{
        sort:{
            sortOrder:1 // set default sort by sortWeight
        }
    },
    indexes:{
        sortOrder:{ 'sortOrder':1 }
    }
});

Orderable.on('beforeCreate', 'checkSortOrder', function(next){
    var doc = this;
    
    if(!doc.sortOrder){
        var now = new Date();
        doc.createdDT = now;
        doc.sortOrder = now.getTime();
        next();
    }
    else {
        doc
        .constructor
        .collection()
        .find({ sortOrder: doc.sortOrder, id:{ $ne: doc.id } })
        .exists(function(err, exists){
            if(err) next(err);
            else {
                doc._sortOrderChanged = exists;
                next();
            }
        });
    }
});

Orderable.on('beforeUpdate', 'checkSortOrder', function(next){
    var doc = this;
    
    if(doc.sortOrder) {
        doc
        .constructor
        .collection()
        .find({ sortOrder: doc.sortOrder, id:{ $ne: doc.id } })
        .exists(function(err, exists){
            if(err) next(err);
            else {
                doc._sortOrderChanged = exists;
                next();
            }
        });
    }
    else next();
});

Orderable.on('afterCreate', 'updateSortOrders', updateSortOrders);
Orderable.on('afterUpdate', 'updateSortOrders', updateSortOrders);

function updateSortOrders(args, next){
    var doc = args[1] || this;
    
    if(this._sortOrderChanged){
        doc
        .constructor
        .collection()
        .find({ sortOrder: { $gte:doc.sortOrder }, id:{ $ne: doc.id } })
        .update({ $inc:{ sortOrder: 1 }}, function(err){
            if(err) next(new Error('Orderable: updating sortOrder failed').cause(err));
            else next();
        });
    }
    else next();
}