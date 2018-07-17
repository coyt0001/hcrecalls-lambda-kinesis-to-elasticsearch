/**
 * @file index.js
 * @description Elasticsearch Stream loader for HC Recalls Data
 * @author Paul Coyte
 * Last-Modified: July 17, 2018
 */

const 
  R = require('ramda'),
  AWS = require('aws-sdk'),
  path = require('path');

/**
 * @class RecordProcessor
 * @description Manages uploading records to elasticsearch domain
 * @prop {Object} records Records to be processed
 * @prop {Object} endpoint AWS endpoint
 * @prop {String} index AWS index
 * @prop {String} doctype AWS index
 * @prop {Object} credentials AWS credentials object
 * @prop {Number} processed Number of successfully processed records
 * @prop {Array} failed List of records that have failed to process
 * @prop {Object} context Handler context reference
 * @prop {Function} processedSuccessfully Incriments processed counter
 * @prop {Function} processedUnsuccessfully Adds record to unprocessed list
 * @prop {Function} handleRequest Async wrapper for AWS.NodeHttpClient.handleRequest
 * @prop {Function} run Runs request manager, uploading records to AWS ES service
 */
class RecordProcessor {
  constructor(records, config, context) {
    this.records = records;
    this.region = config.region;
    this.endpoint = new AWS.endpoint(config.endpoint);
    this.index = config.index;
    this.doctype = config.doctype;
    this.credentials = new AWS.EnvironmentCredentials('AWS');
    this.processed = 0;
    this.failed = [],
    this.context = context;
  }

  /**
   * @method processedSuccessfully
   * @description Incriments processed counter
   */
  processedSuccessfully() { this.processed++; }
  
  /**
   * @method processedSuccessfully
   * @description Incriments processed counter
   */
  processedUnsuccessfully(doc) { this.failed.push(doc); }

  /**
   * @method handleRequest
   * @description Async wrapper for AWS.NodeHttpClient.handleRequest
   * @param {Object} req AWS.HttpRequest to be processed
   * @param {Object} opts AWS.NodeHttpClient.handleRequest options, defaults to null
   * @returns {Promise} Response body or error
   */
  async handleRequest(req, opts = null) {
    const sender = new AWS.NodeHttpClient();

    return new Promise((resolve, reject) => {
      sender.handleRequest(req, opts, httpResponse => {
        let _respBody = '';
        httpResponse.on('data', chunk => { _respBody += chunk; });
        httpResponse.on('end', chunk => { resolve(_respBody); });
      }, reject);
    })
  }

  /**
   * @method run
   * @description Runs request manager
   * @returns 
   */
  async run() {
    const { 
      records,
      region,
      endpoint,
      index,
      doctype,
      credentials,
      handleRequest,
      processedSuccessfully,
      processedUnsuccessfully
    } = this;

    await Promise.all(R.map(async _record => {
      // Create request
      const body = (new Buffer(_record.kinesis.data, 'base64')).toString();
      let _request = new AWS.HttpRequest(endpoint);
      
      _request = R.merge({
        method: 'POST',
        path: path.join('/', index, doctype),
        region,
        headers: R.merge({
          'presigned-expires': false,
          Host: endpoint.host
        }, _request.headers),
        body
      }, _request);
      
      // Sign request
      const _signer = new AWS.Signers.v4(_request, "es");
  
      _signer.addAuthorization(credentials, new Date());
  
      // Make request
      try {
        const _result = await handleRequest(_request);
        context.succeed(`Added document: '${body}'`);
        processedSuccessfully();
        return _result;
      }
      catch(err) {
        context.fail(`Failed to add document: '${body}'`)
        processedUnsuccessfully(body);
        return err;
      }
    }, records));

    return {
      processed: this.processed,
      failed: this.failed.length
    };
  }
}

/**
 * @method handler AWS Lambda handler
 * @param {Object} event
 * @param {Object} context
 */
exports.handler = async (event, context) => {
  // Define request configuration
  const
    awsConfig = {
      region: "us-east-1",
      endpoint: "https://vpc-hcrecalls-tcxfczyviepj52vmx2h4wa7hmm.us-east-1.es.amazonaws.com",
      index: "lambda-kine-index",
      doctype: "lambda-kine-type"
    },
    
    // Get records from event
    { Records } = event;

  // Process records
  const 
    _processor = new RecordProcessor(Records, awsConfig, context),
    { processed, failed } = await _processor.run();

  // Return message detailing # of total processed records
  return console.log(`Processed '${processed}' records.${failed ? ` \nThere were '${failed}' records that failed to process.` : ''}`);
};
