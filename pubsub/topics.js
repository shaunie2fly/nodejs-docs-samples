// Copyright 2016, Google, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

var pubsubClient = require('@google-cloud/pubsub')();

// [START pubsub_create_topic]
function createTopic (topicName, callback) {
  // Creates a new topic, e.g. "my-new-topic"
  pubsubClient.createTopic(topicName, function (err, topic) {
    if (err) {
      callback(err);
      return;
    }

    console.log('Created topic: %s', topic.name);
    callback();
  });
}
// [END pubsub_create_topic]

// [START pubsub_delete_topic]
function deleteTopic (topicName, callback) {
  // References an existing topic, e.g. "my-topic"
  var topic = pubsubClient.topic(topicName);

  // Deletes the topic
  topic.delete(function (err) {
    if (err) {
      callback(err);
      return;
    }

    console.log('Deleted topic: %s', topic.name);
    callback();
  });
}
// [END pubsub_delete_topic]

// [START pubsub_publish_message]
function publishMessage (topicName, message, callback) {
  // References an existing topic, e.g. "my-topic"
  // See https://googlecloudplatform.github.io/google-cloud-node/#/docs/google-cloud/latest/pubsub?method=topic
  var topic = pubsubClient.topic(topicName);

  /**
   * Publish a message to the topic, e.g. { "data": "Hello, world!" }. In
   * Node.js, a PubSub message requires a "data" property, which can have a
   * string or an object as its value. An optional "attributes" property can be
   * an object of key/value pairs, where the keys and values are both strings.
   * See https://cloud.google.com/pubsub/reference/rpc/google.pubsub.v1#google.pubsub.v1.PubsubMessage
   *
   * Topic#publish() takes either a single message object or an array of message
   * objects. See https://googlecloudplatform.github.io/google-cloud-node/#/docs/pubsub/latest/pubsub/topic?method=publish
   */
  topic.publish(message, function (err, messageIds) {
    if (err) {
      callback(err);
      return;
    }

    console.log('Published message: %s', JSON.stringify(message));
    callback();
  });
}
// [END pubsub_publish_message]

// [START pubsub_list_topics]
function listTopics (callback) {
  // Lists all topics in the current project
  pubsubClient.getTopics(function (err, topics) {
    if (err) {
      callback(err);
      return;
    }

    console.log('Topics:');
    topics.forEach(function (topic) {
      console.log(topic.name);
    });
    callback();
  });
}
// [END pubsub_list_topics]

// The command-line program
var cli = require('yargs');
var makeHandler = require('../utils').makeHandler;

var program = module.exports = {
  createTopic: createTopic,
  deleteTopic: deleteTopic,
  publishMessage: publishMessage,
  listTopics: listTopics,
  main: function (args) {
    // Run the command-line program
    cli.help().strict().parse(args).argv;
  }
};

cli
  .demand(1)
  .command('create <topicName>', 'Creates a new topic.', {}, function (options) {
    program.createTopic(options.topicName, makeHandler(false));
  })
  .command('list', 'Lists all topics in the current project.', {}, function (options) {
    program.listTopics(makeHandler(false));
  })
  .command('publish <topicName> <message>', 'Publishes a message.', {}, function (options) {
    try {
      options.message = JSON.parse(options.message);
      program.publishMessage(options.topicName, options.message, makeHandler(false));
    } catch (err) {
      return console.error('"message" must be a valid JSON string!');
    }
  })
  .command('delete <topicName>', 'Deletes the a topic.', {}, function (options) {
    program.deleteTopic(options.topicName, makeHandler(false));
  })
  .example('node $0 create greetings', 'Creates a new topic named "greetings".')
  .example('node $0 list', 'Lists all topics in the current project.')
  .example('node $0 publish greetings \'{"data":"Hello world!"}\'', 'Publishes a message.')
  .example('node $0 delete greetings', 'Deletes a topic named "greetings".')
  .wrap(120)
  .recommendCommands()
  .epilogue('For more information, see https://cloud.google.com/pubsub/docs');

if (module === require.main) {
  program.main(process.argv.slice(2));
}
