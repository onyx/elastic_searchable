require 'active_record'
require 'after_commit'
require 'elastic_searchable/queries'
require 'elastic_searchable/callbacks'
require 'elastic_searchable/index'

module ElasticSearchable
  module ActiveRecordExtensions
    # Valid options:
    # :index (optional) configure index to store data in.  default to ElasticSearchable.default_index
    # :type (optional) configue type to store data in.  default to model table name
    # :index_options (optional) configure index properties (ex: tokenizer)
    # :mapping (optional) configure field properties for this model (ex: skip analyzer for field)
    # :if (optional) reference symbol/proc condition to only index when condition is true
    # :unless (optional) reference symbol/proc condition to skip indexing when condition is true
    # :json (optional) configure the json document to be indexed (see http://api.rubyonrails.org/classes/ActiveModel/Serializers/JSON.html#method-i-as_json for available options)
    def elastic_searchable(options = {})
      cattr_accessor :elastic_options
      self.elastic_options = options.symbolize_keys.merge(:unless => Array.wrap(options[:unless]).push(:elasticsearch_offline?))

      extend ElasticSearchable::Indexing::ClassMethods
      extend ElasticSearchable::Queries

      include ElasticSearchable::Indexing::InstanceMethods
      include ElasticSearchable::Callbacks::InstanceMethods

      define_callbacks :after_index_on_create, :after_index_on_update, :after_index
      after_commit_on_create :update_index_on_create, :if => :should_index?
      after_commit_on_update :update_index_on_update, :if => :should_index?
      after_commit_on_destroy :delete_from_index
    end
  end
end

ActiveRecord::Base.send(:extend, ElasticSearchable::ActiveRecordExtensions)
