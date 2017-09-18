'use strict';

/*
 * Expose Model
 */
module.exports = require('./lib/model.js');

/*
 * Expose all modules
 */
module.exports.cache = require('./lib/cache.js');
module.exports.register = require('./lib/register.js');
module.exports.relations = require('./lib/relations.js');

/*
 * Load model extensions
 */
require('./lib/extensions/defaults.js');
require('./lib/extensions/methods.js');
require('./lib/extensions/queries.js');
require('./lib/extensions/hooks.js');
require('./lib/extensions/validations.js');

/*
 * Load behaviours models
 */
require('./lib/behaviours/Tree.js');
require('./lib/behaviours/Orderable.js');

/*
 * Load datasource models
 */
require('./lib/datasources/DataSource.js');
require('./lib/datasources/Memory.js');
require('./lib/datasources/JsonFile.js');
require('./lib/datasources/Mongo.js');
require('./lib/datasources/Rest.js');
require('./lib/datasources/RestOAuth2.js');
require('./lib/datasources/ElasticSearch.js');
require('./lib/datasources/FileSystem.js');