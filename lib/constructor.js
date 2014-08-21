

var Store=require('./store');
var redis=require('redis');
var connectRedis=require('connect-redis');
module.exports=function(){
    this.createClient=function(port,host,options){
        this.host=host;
        this.port=port;
        this.options=options;
        return redis.createClient(port,host,options);
    };

    this.store=function(client,namespace,idProp){
        return new Store(client,namespace,idProp);
    };
    this.sessionStore=function(elliptical){
        var sessionStore=connectRedis(elliptical);
        var params={};
        if(this.host !==undefined){
            params.host=this.host;
        }
        if(this.port !== undefined){
            params.port=this.port;
        }
        return new sessionStore(params);
    }
};
