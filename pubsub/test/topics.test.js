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

var proxyquire = require('proxyquire').noCallThru();
var topicName = 'foo';
var message = { data: 'Hello, world!' };

function getSample () {
  var apiResponseMock = {};
  var topicMock = {
    get: sinon.stub(),
    publish: sinon.stub().yields(null, [1], apiResponseMock),
    delete: sinon.stub().yields(null, apiResponseMock)
  };
  topicMock.get.yields(null, topicMock, apiResponseMock);
  var topicsMock = [
    {
      name: topicName
    }
  ];

  var pubsubMock = {
    topic: sinon.stub().returns(topicMock),
    getTopics: sinon.stub().yields(null, topicsMock)
  };
  var PubSubMock = sinon.stub().returns(pubsubMock);

  return {
    program: proxyquire('../topics', {
      '@google-cloud/pubsub': PubSubMock
    }),
    mocks: {
      PubSub: PubSubMock,
      pubsub: pubsubMock,
      topics: topicsMock,
      topic: topicMock,
      apiResponse: apiResponseMock
    }
  };
}

describe('pubsub:topics', function () {
  describe('createTopic', function () {
    it('should create a topic', function () {
      var sample = getSample();
      var callback = sinon.stub();

      sample.program.createTopic(topicName, callback);

      assert.equal(sample.mocks.topic.get.calledOnce, true);
      assert.deepEqual(sample.mocks.topic.get.firstCall.args.slice(0, -1), [{
        autoCreate: true
      }]);
      assert.equal(callback.calledOnce, true);
      assert.deepEqual(callback.firstCall.args, [null, sample.mocks.topic, sample.mocks.apiResponse]);
      assert.equal(console.log.calledOnce, true);
      assert.deepEqual(console.log.firstCall.args, ['Created topic: %s', topicName]);
    });

    it('should handle error', function () {
      var sample = getSample();
      var error = new Error('error');
      var callback = sinon.stub();
      sample.mocks.topic.get.yields(error);

      sample.program.createTopic(topicName, callback);

      assert.equal(callback.calledOnce, true);
      assert.deepEqual(callback.firstCall.args, [error]);
    });
  });

  describe('deleteTopic', function () {
    it('should delete a topic', function () {
      var sample = getSample();
      var callback = sinon.stub();

      sample.program.deleteTopic(topicName, callback);

      assert.equal(sample.mocks.topic.delete.calledOnce, true);
      assert.deepEqual(sample.mocks.topic.delete.firstCall.args.slice(0, -1), []);
      assert.equal(callback.calledOnce, true);
      assert.deepEqual(callback.firstCall.args, [null, sample.mocks.apiResponse]);
      assert.equal(console.log.calledOnce, true);
      assert.deepEqual(console.log.firstCall.args, ['Deleted topic: %s', topicName]);
    });

    it('should handle error', function () {
      var sample = getSample();
      var error = new Error('error');
      var callback = sinon.stub();
      sample.mocks.topic.delete.yields(error);

      sample.program.deleteTopic(topicName, callback);

      assert.equal(callback.calledOnce, true);
      assert.deepEqual(callback.firstCall.args, [error]);
    });
  });

  describe('publish', function () {
    it('should publish a message', function () {
      var sample = getSample();
      var callback = sinon.stub();

      sample.program.publishMessage(topicName, message, callback);

      assert.equal(sample.mocks.topic.publish.calledOnce, true);
      assert.deepEqual(sample.mocks.topic.publish.firstCall.args.slice(0, -1), [message]);
      assert.equal(callback.calledOnce, true);
      assert.deepEqual(callback.firstCall.args, [null, [1], sample.mocks.apiResponse]);
      assert.equal(console.log.calledOnce, true);
      assert.deepEqual(console.log.firstCall.args, ['Published %d message(s)!', 1]);
    });

    it('should handle error', function () {
      var sample = getSample();
      var error = new Error('error');
      var callback = sinon.stub();
      sample.mocks.topic.publish.yields(error);

      sample.program.publishMessage(topicName, message, callback);

      assert.equal(callback.calledOnce, true);
      assert.deepEqual(callback.firstCall.args, [error]);
    });
  });

  describe('publishOrderedMessage', function () {
    it('should publish ordered messages', function () {
      var sample = getSample();
      var callback = sinon.stub();

      sample.program.publishOrderedMessage(topicName, message, callback);

      assert.equal(sample.mocks.topic.publish.calledOnce, true);
      assert.deepEqual(sample.mocks.topic.publish.firstCall.args.slice(0, -1), [{
        data: message.data,
        messageId: 1
      }]);
      assert.equal(callback.calledOnce, true);
      assert.deepEqual(callback.firstCall.args, [null, [1], sample.mocks.apiResponse]);
      assert.equal(console.log.calledOnce, true);
      assert.deepEqual(console.log.firstCall.args, ['Published %d message(s)!', 1]);

      sample.program.publishOrderedMessage(topicName, message, callback);

      assert.equal(sample.mocks.topic.publish.calledTwice, true);
      assert.deepEqual(sample.mocks.topic.publish.secondCall.args.slice(0, -1), [{
        data: message.data,
        messageId: 2
      }]);
      assert.equal(callback.calledTwice, true);
      assert.deepEqual(callback.secondCall.args, [null, [1], sample.mocks.apiResponse]);
      assert.equal(console.log.calledTwice, true);
      assert.deepEqual(console.log.secondCall.args, ['Published %d message(s)!', 1]);
    });

    it('should handle error', function () {
      var sample = getSample();
      var error = new Error('error');
      var callback = sinon.stub();
      sample.mocks.topic.publish.yields(error);

      sample.program.publishOrderedMessage(topicName, message, callback);

      assert.equal(callback.calledOnce, true);
      assert.deepEqual(callback.firstCall.args, [error]);
    });
  });

  describe('list', function () {
    it('should list topics', function () {
      var sample = getSample();
      var callback = sinon.stub();

      sample.program.listTopics(callback);

      assert.equal(sample.mocks.pubsub.getTopics.calledOnce, true);
      assert.deepEqual(sample.mocks.pubsub.getTopics.firstCall.args.slice(0, -1), []);
      assert.equal(callback.calledOnce, true);
      assert.deepEqual(callback.firstCall.args, [null, sample.mocks.topics]);
      assert.equal(console.log.calledOnce, true);
      assert.deepEqual(console.log.firstCall.args, ['Found %d topics!', sample.mocks.topics.length]);
    });

    it('should handle error', function () {
      var sample = getSample();
      var error = new Error('error');
      var callback = sinon.stub();
      sample.mocks.pubsub.getTopics.yields(error);

      sample.program.listTopics(callback);

      assert.equal(callback.calledOnce, true);
      assert.deepEqual(callback.firstCall.args, [error]);
    });
  });

  describe('main', function () {
    it('should call createTopic', function () {
      var program = getSample().program;

      sinon.stub(program, 'createTopic');
      program.main(['create', topicName]);
      assert.equal(program.createTopic.calledOnce, true);
      assert.deepEqual(program.createTopic.firstCall.args.slice(0, -1), [topicName]);
    });

    it('should call deleteTopic', function () {
      var program = getSample().program;

      sinon.stub(program, 'deleteTopic');
      program.main(['delete', topicName]);
      assert.equal(program.deleteTopic.calledOnce, true);
      assert.deepEqual(program.deleteTopic.firstCall.args.slice(0, -1), [topicName]);
    });

    it('should call listTopics', function () {
      var program = getSample().program;

      sinon.stub(program, 'listTopics');
      program.main(['list']);
      assert.equal(program.listTopics.calledOnce, true);
      assert.deepEqual(program.listTopics.firstCall.args.slice(0, -1), []);
    });

    it('should call publishMessage', function () {
      var program = getSample().program;

      sinon.stub(program, 'publishMessage');
      program.main(['publish', topicName, '{}']);
      assert.equal(program.publishMessage.calledOnce, true);
      assert.deepEqual(program.publishMessage.firstCall.args.slice(0, -1), [topicName, {}]);
    });

    it('should call publishMessage and validate message', function () {
      var program = getSample().program;

      sinon.stub(program, 'publishMessage');
      program.main(['publish', topicName, '{asdf}']);
      assert.equal(program.publishMessage.calledOnce, false);
      assert.equal(console.error.calledOnce, true);
      assert.deepEqual(console.error.firstCall.args, ['"message" must be a valid JSON string!']);
    });
  });
});
