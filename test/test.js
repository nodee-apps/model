
/*
 * run all tests
 */

require('./cache.js');
require('./model.js');
require('./datasources/memoryDS.js');
require('./relations.js');

// tests require r/w access to jsonData.json file
require('./datasources/jsonFileDS.js');

// behaviours test
require('./behaviours/orderable.js');
require('./behaviours/tree.js');

// tests require filesystem
// require('./datasources/fileSystemDS.js');

// test require connection to mongodb
// require('./datasources/mongoDS.js');

// test require connection to elasticSearch
// require('./datasources/elasticSearchDS.js');