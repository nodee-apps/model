'use strict';

var Model = require('../model.js'),
    object = require('nodee-utils').object,
    request = require('nodee-utils').request,
    jwt = require('nodee-utils').jwt;

/*
 * RestOAuth2 data source:
 */
var RestOAuth2 = module.exports = Model.define('RestOAuth2DataSource', [ 'RestDataSource' ], {

});

/*
 * defaults
 */
RestOAuth2.extendDefaults({
    connection:{
        // debugRequest: true, // debug requests, only for dev
        // debugResponse: true, // debug response, only for dev
        // baseUrl:'',

        oauth:{
            // disabled: true, // oauth can be disabled, useful if testing datasource on local dev
            accessToken:{
                value: '', // stored token value
                expires: 0, // stored token expiration timestamp
                url: 'api.endpoint.com',
                body:[], // e.g. 'grant_type=client_credentials'
                username: '',
                password: '',
                tokenKey: 'access_token',
                expireKey: 'expires_in',
                // jwt:{
                //     iss: '',
                //     scope: 'https://www.googleapis.com/auth/datastore',
                //     aud: 'https://www.googleapis.com/oauth2/v4/token',
                //     exp: 30*60,
                //     privateKey: '-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n',
                //     type: 'RS256'
                // }
            }
        },
        rejectUnauthorized: true, // by default, only verified certificates
    },
    cache:{
        keyPrefix:'nodee-model-rest-oauth2',
        // createKey: createCacheKey - using createKey defined in DataSource
        // duration: 3000, // duration (undefined or zero means no expiration)
        // use: false, // by default do not use cache
    }
});

/*
 * Request / response auth interceptors
 */

RestOAuth2.requestInterceptor = function(defaults, origRequest, done){
    var ModelCnst = this,
        accessToken = defaults.connection.oauth.accessToken,
        authRequest;

    if(defaults.connection.oauth.disabled) return done();

    var nowSec = Math.floor( new Date().getTime()/1000 - 10 ); // 10 seconds before expiration
    if(!accessToken.value || nowSec < accessToken.expires) {

        authRequest = request.post(accessToken.url);

        for(var i=0;i<accessToken.body.length;i++){
            authRequest.send(accessToken.body[i]);
        }

        if(accessToken.jwt) {
            authRequest.send('assertion=' + jwt.encode({
                iss: accessToken.jwt.iss,
                scope: accessToken.jwt.scope,
                aud: accessToken.jwt.aud,
                exp: nowSec + (accessToken.jwt.exp || 30*60),
                iat: nowSec,
            }, accessToken.jwt.privateKey, accessToken.jwt.type || 'RS256'));
        }
        else if(accessToken.username) authRequest.auth(accessToken.username, accessToken.password);

        authRequest.end(function(err, res){
            if(err) return done(new Error('OAuth2 Failed: returned status "'+(res ? res.status : 'undefined')+'", body "' + JSON.stringify(res ? res.body || res.text : '') + '"').cause(err));
            else if(res.status !== 200) return done(new Error('OAuth2 Failed: returned status "'+res.status+'", body "' +JSON.stringify(res.body || res.text)+ '"'));
            else {
                var resBody = res.body || res.text;
                var at = object.deepGet(resBody, accessToken.tokenKey);
                var exp = parseInt( object.deepGet(resBody, accessToken.expireKey) );

                if(!at) return done(new Error('OAuth2 Failed: cannot parse access token by key "'+accessToken.tokenKey+'", body "' +JSON.stringify(resBody)+ '"'));
                if(!exp) return done(new Error('OAuth2 Failed: cannot parse access token expiration by key "'+accessToken.expireKey+'", body "' +JSON.stringify(resBody)+ '"'));

                updateAccessToken(ModelCnst, defaults, at, exp);
                origRequest.set('Authorization','Bearer '+at);
                done();
            }
        });

    }
    else {
        origRequest.set('Authorization','Bearer '+accessToken.value);
        done();
    }
};

RestOAuth2.responseInterceptor = function(defaults, repeatsCount, err, res, done){
    var ModelCnst = this;

    if(defaults.connection.oauth.disabled) return done();

    if(res && (res.status === 401 || res.status === 403)){
        if(!repeatsCount) {
            // reset access token
            updateAccessToken(ModelCnst, defaults, '', 0);
            return done(null, true);
        }
    }
    done();
};

function updateAccessToken(ModelCnst, defaults, at, exp){
    // update constructor access token
    ModelCnst.extendDefaults({
        connection:{
            oauth:{
                accessToken:{
                    value: at, // stored token value
                    expires: exp, // stored token expiration timestamp
                }
            }
        }
    });

    // update instance access token
    defaults.connection.oauth.accessToken.value = at;
    defaults.connection.oauth.accessToken.expires = exp;
}

RestOAuth2.addRequestInterceptor(RestOAuth2.requestInterceptor);
RestOAuth2.addResponseInterceptor(RestOAuth2.responseInterceptor);

/*
 * Model instance methods - inherited from DataSource
 */