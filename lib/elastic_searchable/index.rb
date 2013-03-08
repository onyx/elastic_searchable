require 'will_paginate'

module ElasticSearchable
  module Indexing
    module ClassMethods
      # delete all documents of this type in the index
      # http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/delete_mapping/
      def clean_index
        ElasticSearchable.request :delete, index_type_path
      end

      # configure the index for this type
      # http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/put_mapping/
      def update_index_mapping
        if mapping = self.elastic_options[:mapping]
          create_index unless index_exists?
          ElasticSearchable.request :put, index_type_path('_mapping'), :json_body => {index_type => mapping}
        end
      end

      def index_exists?
        ElasticSearchable.request(:get, '/_status')['indices'].include?(index_name)
      end

      # create the index
      # http://www.elasticsearch.org/guide/reference/api/admin-indices-create-index.html
      def create_index
        options = {}
        options.merge! :settings => self.elastic_options[:index_options] if self.elastic_options[:index_options]
        options.merge! :mappings => {index_type => self.elastic_options[:mapping]} if self.elastic_options[:mapping]
        ElasticSearchable.request :put, index_path, :json_body => options
      end

      # explicitly refresh the index, making all operations performed since the last refresh
      # available for search
      #
      # http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/refresh/
      def refresh_index
        ElasticSearchable.request :post, index_path('_refresh')
      end

      # deletes the entire index
      # http://www.elasticsearch.com/docs/elasticsearch/rest_api/admin/indices/delete_index/
      def delete_index
        ElasticSearchable.request :delete, index_path
      end

      # delete one record from the index
      # http://www.elasticsearch.com/docs/elasticsearch/rest_api/delete/
      def delete_id_from_index(id)
        ElasticSearchable.request :delete, index_type_path(id)
      rescue ElasticSearchable::ElasticError => e
        ElasticSearchable.logger.warn e
      end

      # helper method to generate elasticsearch url for this object type
      def index_type_path(action = nil)
        index_path [index_type, action].compact.join('/')
      end

      # helper method to generate elasticsearch url for this index
      def index_path(action = nil)
        ['', index_name, action].compact.join('/')
      end

      # reindex all records using bulk api
      # see http://www.elasticsearch.org/guide/reference/api/bulk.html
      # options:
      #   :scope - scope to use for looking up records to reindex. defaults to self (all)
      #   :page - page/batch to begin indexing at. defaults to 1
      #   :per_page - number of records to index per batch. defaults to 1000
      def reindex(options = {})
        self.update_index_mapping
        options.reverse_merge! :page => 1, :per_page => 1000, :total_entries => 1
        scope = options.delete(:scope) || self

        errors = []
        records = scope.paginate(options)
        while records.any? do
          ElasticSearchable.logger.debug "reindexing batch ##{records.current_page}..."

          actions = []
          records.each do |record|
            next unless record.should_index?
            begin
              doc = ElasticSearchable.encode_json(record.as_json_for_index)
              actions << ElasticSearchable.encode_json({:index => {'_index' => index_name, '_type' => index_type, '_id' => record.id}})
              actions << doc
            rescue => e
              errors << record.id
              ElasticSearchable.logger.warn "Unable to bulk index record: #{record.inspect} [#{e.message}]"
            end
          end

          begin
            ElasticSearchable.request(:put, '/_bulk', :body => "\n#{actions.join("\n")}\n") if actions.any?
          rescue ElasticError => e
            errors = errors | records.map(&:id)
            ElasticSearchable.logger.warn "Error indexing batch ##{options[:page]}: #{e.message}"
            ElasticSearchable.logger.warn e
          end

          options.merge! :page => (options[:page] + 1)
          records = scope.paginate(options)
        end

        errors
      end

      def disable_refresh
        set_refresh_interval '-1'
      end

      def enable_refresh
        set_refresh_interval '1'
      end

      def optimize
        ElasticSearchable.request :post, index_path('_optimize')
      end

      private

      def set_refresh_interval interval
        refresh_settings = { :index => { :refresh_interval => interval }}
        ElasticSearchable.request :put, index_path('_settings'), :body => ElasticSearchable.encode_json(refresh_settings)
      end

      def index_name
        self.elastic_options[:index] || ElasticSearchable.default_index
      end
      def index_type
        self.elastic_options[:type] || self.table_name
      end
    end

    module InstanceMethods
      # reindex the object in elasticsearch
      # fires after_index callbacks after operation is complete
      # see http://www.elasticsearch.org/guide/reference/api/index_.html
      def reindex(lifecycle = nil)
        query = {}
        query.merge! :percolate => "*" if self.class.elastic_options[:percolate]
        response = ElasticSearchable.request :put, self.class.index_type_path(self.id), :query => query, :json_body => self.as_json_for_index

        self.run_callbacks("after_index_on_#{lifecycle}".to_sym) if lifecycle
        self.run_callbacks(:after_index)

        if percolate_callback = self.class.elastic_options[:percolate]
          matches = response['matches']
          self.send percolate_callback, matches if matches.any?
        end
      end
      # document to index in elasticsearch
      def as_json_for_index
        self.as_json self.class.elastic_options[:json]
      end
      def should_index?
        [self.class.elastic_options[:if]].flatten.compact.all? { |m| evaluate_elastic_condition(m) } &&
        ![self.class.elastic_options[:unless]].flatten.compact.any? { |m| evaluate_elastic_condition(m) }
      end
      # percolate this object to see what registered searches match
      # can be done on transient/non-persisted objects!
      # can be done automatically when indexing using :percolate => true config option
      # http://www.elasticsearch.org/blog/2011/02/08/percolator.html
      def percolate
        response = ElasticSearchable.request :get, self.class.index_type_path('_percolate'), :json_body => {:doc => self.as_json_for_index}
        response['matches']
      end

      private
      def elasticsearch_offline?
        ElasticSearchable.offline?
      end
      # ripped from activesupport
      def evaluate_elastic_condition(method)
        case method
          when Symbol
            self.send method
          when String
            eval(method, self.instance_eval { binding })
          when Proc, Method
            method.call
          else
            if method.respond_to?(kind)
              method.send kind
            else
              raise ArgumentError,
                "Callbacks must be a symbol denoting the method to call, a string to be evaluated, " +
                "a block to be invoked, or an object responding to the callback method."
            end
        end
      end
    end
  end
end
