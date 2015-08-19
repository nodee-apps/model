
![MIT License][license-image]
[![Build Status][travis-image]][travis-url]

# Advanced, database agnostic ODM

Sometimes your application is not a simple to-do list and you need to write complex business logic.
Rather than strictly predefined CRUD methods and hooks, enterprise-model is a set of tools which you can use to write your own logic, data sources and reusable behaviours.
* Scheme (inheritable, with nested models)
* Validations (extendable validations and sanitizers)
* Defaults (inheritable default settings)
* Methods (inheritable instance and constructor methods)
* Queries (inheritable, extendable and cacheable query builder methods)
* Hooks (inheritable and extendable)
* Relations with integrity maintaining hooks
* Data sources with optimistic locks (Memory, Json file, Mongo, Rest, Elastic search)
* Caching (synchronize workers' cache across nodejs cluster)
* Behaviours (Orderable, Tree)

## Installation
```
npm install enterprise-model
```

## Usage

```javascript
var Model = require('enterprise-model');

/*
 * create employee model, which:
 * 1. is stored in mongo database
 * 2. is in tree structure
 * 3. is orderable
 * 4. can handle process of changing job - it is not just a simple update,
 * it has to be confirmed by HR department
 */
var Employee = Model.define('Employee', [ 'MongoDataSource', 'Orderable', 'Tree' ], {
    name:{ isString:true },
    surname:{ isString:true },
    salary:{ isNumber:true, round:2 },
    job:{ isIn:[ 'project_manager', 'sales', 'support'] },
    jobConfirmed:{ isBool:true },
    // address as submodel
    address: { model: Model('Address') },
    // or array of submodels
    addresses: { arrayOf: Model('Address') }
});

// define connection details by extending inherited defaults
Employee.extendDefaults({
    connection:{
        host: 'localhost',
        port: 27017,
        database:'myapp',
        collection:'employees'
    }
});

// now, add hookable method
Employee.prototype.changeJob = Employee.wrapHooks('changeJob', function(newJob, cb){
    var employee = this;
    employee.job = newJob;
    employee.jobConfirmed = false;
    employee.update(cb);
});

// register "beforeChangeJob" listener
Employee.on('beforeChangeJob', function(next){
    // notify HR department
    next();
});

// init model, this will ensure indexes or do some work to init datastore
Employee.init();

// now we can get employee and change his job
Employee.collection().find({ name:'Chuck', surname:'Norris' }).one(function(err, employee){
    employee.changeJob('super_agent', function(err){
        // job changed, and HR department was notified
        // (but I am sure they can't change Chuck's job - nobody can :)
    });
});

// if you need to define a new type of employee, just inherit it from Employee.
// It will inherit all methods including registered events like "beforeJobChange".
var SuperEmployee = Model.define('SuperEmployee', ['Employee']);
```

[license-image]: https://img.shields.io/badge/license-MIT-blue.svg?style=flat
[license-url]: license.txt

[travis-url]: https://travis-ci.org/nodejs-enterprise/model
[travis-image]: https://travis-ci.org/nodejs-enterprise/model.svg?branch=master