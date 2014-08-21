
/* a rudimentary provider for a redis implementation of a document store
* essentially, we create an index array document
*
* the redis key-value pair for the index:
*      <key:namespace + '_$dbindex',value:index>
*      where:
*      index=[obj1,obj2,...objN], where obj[i]={model:<model>,keys:<[key1,key2,...keyM]>}
*
*      1) 'post' a document: assign a guid as a key for the document, json stringify the payload, client.set(key,value), add
*          the key to the index document
*      2) 'put' a document: get the existing doc by key, merge with the new, client.set
*      3) 'delete', client.del, remove key from the index
*      4) 'get', if an id is passed in the params, client.get(id), if not, get the model keys from the index, client.mget(keys)
*      5) we also implement mset(pairArray, model),flushModel,flushAll(restricted to the store namespace)
* */




var elliptical=require('elliptical-mvc');
var utils=require('elliptical-utils');

var _=utils._;



module.exports=elliptical.Provider.extend({

},{
    init:function(client,namespace,idProp){
        this.client=client;
        this.namespace=namespace;
        this.index=this.namespace + '_$dbindex';
        this.idProp=(idProp===undefined) ? 'id' : idProp;
        _onInit(this);

    },

    get: function(params,model,callback){
        var id=params[this.idProp];
        if(typeof id==='undefined'){
            /* get all */
            this.getAll(model,callback);
        }else{
            /* get by id */
            this.getByKey(id,callback);
        }
    },

    getByKey: function (key, callback) {
        var client=this.client;
        key=this.namespace + '_' + key;
        client.get(key,callback);
    },

    mget:function(keys,callback){
        var client=this.client;
        client.mget(keys,callback);
    },

    getAll:function(model,callback){
       var client=this.client;
        _getModelIndex(model,this,function(err,data){
            var keyArray=data.modelKeysObj.keys;
            client.mget(keyArray,callback);
        });
    },


    /**
     *
     * @param params {Object}
     * @param model {String}
     * @param callback {Function}
     * @returns callback
     * @public
     */
    post: function(params,model,callback){
        var id=utils.guid();
        params[this.idProp]=id;
        this.set(id,params,model,callback);
    },

    /**
     *
     * @param params {Object}
     * @param model {String}
     * @param callback {Function}
     * @returns callback
     * @public
     */
    put: function(params,model,callback){
        var id=params[this.idProp];
        this.set(id,params,model,callback);
    },

    /**
     *
     * @param params {Object}
     * @param model {String}
     * @param callback {Function}
     * @returns callback
     * @public
     */
    delete: function(params,model,callback){
        var id=params[this.idProp];
        this.remove(id,model,callback);
    },

    /**
     *
     * @param key
     * @param val
     * @param model
     * @param callback
     */
    set: function (key, val, model, callback) {
        try {
            var client=this.client;
            _validateKey(key,model,this,function(err,reply){
                var data=reply;
                if(data){
                    data=_.extend(reply, val);
                }
                var data_=JSON.stringify(data);
                client.set(key,data_);
                if(callback){
                    callback(err,data);
                }
            });

        } catch (ex) {
            if (callback) {
                callback(ex,null);
            }
        }
    },

    /**
     *
     * @param pairArray
     * @param model
     * @param callback
     */
    mset: function (pairArray,model, callback) {
        var client=this.client;
        var length = pairArray.length;
        if (length > 0 && length % 2 === 0) {
            var arr=_createObjectList(pairArray);
            var keys=_keyArray(arr);
            _validateKeys(keys,model,this,function(err,data){
                client.mset(pairArray,function(err,reply){
                    if(callback){
                        callback(err,reply);
                    }
                });

            });
        }

    },

    /**
     *
     * @param callback
     */
    length: function (callback) {
        throw new Error('length not implemented');

    },

    /**
     *
     * @param callback
     */
    list: function(callback){
        _getIndex(this,function(err,data){
            if(!err){
                data=JSON.parse(data);
                callback(err,data);
            }else{
                callback(err,null);
            }

        });
    },

    /**
     *
     * @param key
     * @param model
     * @param callback
     */
    remove: function (key,model, callback) {
        var client=this.client;
      _removeModelIndexKey(key,model,this,function(err,data){
          client.del(key);
          if(callback){
              callback(err,null);
          }
      });

    },

    /**
     * remove the all documents of a model type
     * @param model
     * @param callback
     */
    flushModel:function(model,callback){
        var client=this.client;
        _removeModelIndex(model,this,function(err,data){
            client.del(data);
            if(callback){
                callback(err,null);
            }
        });
    },

    /**
     * clears the entire store namespace and index
     * @param callback {Function}
     * @returns callback
     * @public
     */
    flushAll: function (callback) {
        var client=this.client;
        _removeIndex(this,function(err,data){
            client.del(data);
            if(callback){
                callback(err,null);
            }
        });

    },

    /**
     *
     * @param params {Object}
     * @param model {String}
     * @param callback {Function}
     * @returns callback
     * @public
     */
    query: function(params,model,callback){
        throw new Error('query not implemented');
    },

    /**
     *
     * @param params {Object}
     * @param model {String}
     * @param callback {Function}
     * @returns callback
     * @public
     */
    command: function(params,model,callback){
        throw new Error('command not implemented');
    }


});



/**
 *
 * @param context {Object} provider context(this)
 * @private
 */
function _onInit(context){
    var key=context.index;
    var client=context.client;
    client.get(key,function(err,data){
        if(!data){
            _initIndex(context);
        }
    });
}

/**
 *
 * @param context {Object} provider context(this)
 * @private
 */
function _initIndex(context) {
    var key=context.index;
    var val=[];
    val=JSON.stringify(val);
    var client=context.client;
    client.set(key,val);
}



/**
 * get the document store index
 * @param context {Object} provider context(this)
 * @param callback {Function}
 * @private
 */
function _getIndex(context,callback){
    var key=context.index;
    var client=context.client;
    client.get(key,function(err,data){
        callback(err,data);
    });
}


/**
 * if a model key exists, returns the model value, otherwise
 * inserts the key into the model key array or creates a model key array for a model
 * if it does not currently exist
 * @param key {String}
 * @param model {String}
 * @param context {Object} provider context(this)
 * @param callback {Function}
 * @private
 */
function _validateKey(key, model,context,callback) {
    var client=context.client;
    _getModelIndex(model,context,function(err,reply){
        if(reply.modelIndex){
            if(_isModelIndexKey(key,reply.modelKeysObj)){
                client.get(key,function(err,data){
                    var d=JSON.parse(data);
                    callback(null,d);
                });
            }else{
                _insertModelIndexKey(key,reply,context);
                callback(null,null);
            }

        }else{
            _createModelIndex(key,reply,model,context);
            callback(null,null)
        }
    });

}

function _validateKeys(keys, model,context,callback) {

    _getModelIndex(model,context,function(err,reply){
        if(reply.modelIndex){
            _insertModelIndexKey(keys,reply,context);
            callback(null,null);
        }else{
            _createModelIndex(keys,reply,model,context);
            callback(null,null)
        }
    });

}


/**
 * returns the document store index array
 * @param model {String}
 * @param context {Object} provider context(this)
 * @param callback {Function}
 * @private
 */
function _getModelIndex(model,context,callback){

        _getIndex(context,function(err,data){
            if(!err){
                var reply=_parseIndexData(data,model);
                callback(null,reply);
            }else{
                callback(err,null);
            }
        });
}

/**
 * returns object state representing the document store index with respect to a model
 * @param json {String}
 * @param model {String}
 * @returns {Object} the index array, the index of the object of model keys, the object of model keys
 * @private
 */
function _parseIndexData(json,model){
    var index=JSON.parse(json);
    var obj=null;
    var modelIndex=null;
    for (var i = 0, max = index.length; i < max; i++) {
        if (index[i].model === model) {
            obj=index[i];
            modelIndex=i;
            break;
        }
    }

    return {
        index:index,
        modelIndex:modelIndex,
        modelKeysObj:obj
    };
}

/**
 * tests whether a key is a member of a model set of keys
 * @param key {String}
 * @param modelKeysObj {Object}
 * @returns {boolean}
 * @private
 */
function _isModelIndexKey(key,modelKeysObj){
    var keyArray=modelKeysObj.keys;
    var exists=false;
    for (var i = 0, max = keyArray.length; i < max; i++) {
        if (keyArray[i] === key) {
            exists=true;
            break;
        }
    }
    return exists;
}

/**
 * creates a new model keys obj in the store document index
 * @param key {String}\{Array}
 * @param indexObj {Object}
 * @param model {String}
 * @param context {Object} provider context(this)
 * @private
 */

function _createModelIndex(key,indexObj,model,context){
    var indexArray=indexObj.index;
    var modelKeysObj={
        model:model,
        keys:[]
    };

    if(typeof key==='string'){
        modelKeysObj.keys.push(key);
    }else{
        if(key.length){
            modelKeysObj.keys=_.union(modelKeysObj.keys,key);
        }
    }

    indexArray.push(modelKeysObj);
    var index=JSON.stringify(indexArray);
    var indexKey=context.index;
    var client=context.client;
    client.set(indexKey,index);
}


/**
 * delete key from the model index
 * @param key
 * @param indexObj
 * @param context
 * @returns {boolean}
 * @private
 */
function _deleteKey(key,indexObj,context) {
    var indexArray=indexObj.index;
    var modelKeysObj=indexObj.modelKeysObj;
    var modelIndex=indexObj.modelIndex;
    var result=[];
    result=_.remove(modelKeysObj.keys,function(v){
        return v=key;
    });

    indexArray.splice(modelIndex,0);
    indexArray.push(modelKeysObj);

    var index=JSON.stringify(indexArray);

    var indexKey=context.index;
    var client=context.client;
    client.set(indexKey,index);

    return (result.length >0);
}

/**
 * delete the entire model index
 * @param indexObj
 * @param context
 * @private
 */
function _deleteModelIndex(indexObj,context) {
    var indexArray=indexObj.index;
    var modelIndex=indexObj.modelIndex;
    indexArray.splice(modelIndex,0);
    var index=JSON.stringify(indexArray);
    var indexKey=context.index;
    var client=context.client;
    client.set(indexKey,index);
}

function _deleteIndex(context) {
   _initIndex(context);
}

/**
 *
 * @param key {String}
 * @param model {String}
 * @param context {Object}
 * @param callback {Function}
 * @private
 */
function _removeModelIndexKey(key,model,context,callback){
    _getModelIndex(model,context,function(err,reply){
        if(reply.modelIndex){
            if(_deleteKey(key,reply,context)){
                callback(null,null)
            }else{
                callback({
                    statusCode:404,
                    message:'Model Key does not exist'
                },null);
            }
        }
    });

}

/**
 * inserts key/keys into the model index of keys
 * @param key {String}/{Array}
 * @param indexObj {Object}
 * @param context {Object} provider context(this)
 * @private
 */
function _insertModelIndexKey(key,indexObj,context){
    var indexArray=indexObj.index;
    var modelKeysObj=indexObj.modelKeysObj;
    var modelIndex=indexObj.modelIndex;
    if(typeof key==='string'){
        modelKeysObj.keys.push(key);
    }else{
        if(key.length){
            modelKeysObj.keys=_.union(modelKeysObj.keys,key);
        }
    }


    indexArray.splice(modelIndex,0);
    indexArray.push(modelKeysObj);

    var index=JSON.stringify(indexArray);

    var indexKey=context.index;
    var client=context.client;
    client.set(indexKey,index);
}

/**
 * remove the model index from the index
 * @param model {String}
 * @param context {Object}
 * @param callback {Function}
 */
function _removeModelIndex(model,context,callback){
    _getModelIndex(model,context,function(err,reply){
        var keysArr=[];
        if(reply.modelKeysObj){
            var keys=reply.modelKeysObj.keys;
            keys.forEach(function(k){
                keysArr.push(k);
            });
            _deleteModelIndex(reply,context);
        }

        callback(err,keysArr);
    });
}

/**
 * reset the document store index
 * @param context {Object}
 * @param callback {Function}
 * @private
 */
function _removeIndex(context,callback){
    _getIndex(context,function(err,reply){
        var keysArr=[];
        var objArray=JSON.parse(reply);
        if(objArray && objArray.length){
            objArray.forEach(function(obj){
                var keys=objArrays.keys;
                if(keys && keys.length){
                    keys.forEach(function(k){
                        keysArr.push(k);
                    });
                }
            })
        }

        _deleteIndex(context);
        callback(err,keysArr);

    });
}


/**
 * create an array of objects from a pairArray
 * @param pairArray
 * @returns {Array}
 * @private
 */
function _createObjectList(pairArray){
    var objArray=[];
    for (var i = 0, max = pairArray.length; i < max; i++) {
        if(i===0 || i%2===0){
            var obj={};
            obj.key=pairArray[i];
            var j=i+1;
            obj.val=pairArray[j];
            objArray.push(obj);
        }
    }

    return objArray;
}

/**
 *
 * @param objArray {Array}
 * @private
 */
function _keyArray(objArray){
    var keyArray=[];
    objArray.forEach(function(obj){
        keyArray.push(obj.key);
    });

    return keyArray;
}

