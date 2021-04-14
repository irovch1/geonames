var geonames = require('geonames-stream');
var dbclient = require('pelias-dbclient');
var model = require('pelias-model');
var blacklistStream = require('pelias-blacklist-stream');

var featureCodeFilterStream = require('../streams/featureCodeFilterStream');
var adminLookupStream = require('pelias-wof-admin-lookup');
var layerMappingStream = require('../streams/layerMappingStream');
var peliasDocGenerator = require('../streams/peliasDocGenerator');
var overrideLookedUpLocalityAndLocaladmin = require('../streams/overrideLookedUpLocalityAndLocaladmin');

const through = require('through2');
const DUMP_TO = process.env.DUMP_TO;

function createDocumentMapperStream() {
  if(DUMP_TO) {
    return through.obj( function( model, enc, next ){
      next(null, model.callPostProcessingScripts());
    });
  }

  return model.createDocumentMapperStream();
}

module.exports = function( sourceStream, endStream ){
  endStream = endStream || dbclient({name: 'geonames'});

  return sourceStream.pipe( geonames.pipeline )
    .pipe( featureCodeFilterStream.create() )
    .pipe( layerMappingStream.create() )
    .pipe( peliasDocGenerator.create() )
    .pipe( blacklistStream() )
    .pipe( adminLookupStream.create() )
    .pipe( overrideLookedUpLocalityAndLocaladmin.create() )
    .pipe( createDocumentMapperStream())
    .pipe( endStream );
};
