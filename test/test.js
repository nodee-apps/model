
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
require('./datasources/fileSystemDS.js');

// test require connection to mongodb, please use your own databse
require('./datasources/mongoDS.js')('mongodb+srv://model_test_user:please_dont_use_this_db_only_for_testing_purpose@dev-cluster.xatfm.mongodb.net/nodee_model_test?retryWrites=true&w=majority');


// TODO: update elastic search driver to support new elastic server version
// test require connection to elasticSearch, please use your own elasticsearch
// require('./datasources/elasticSearchDS.js')('https://oi6juoqj4w:bhzfvm8a08@nodee-test-9340157225.us-east-1.bonsaisearch.net:443');