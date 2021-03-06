require 'will_paginate/collection'

module ElasticSearchable
  module Queries
    PER_PAGE_DEFAULT = 20

    # search returns a will_paginate collection of ActiveRecord objects for the search results
    # supported options:
    # :page - page of results to search for
    # :per_page - number of results per page
    #
    # http://www.elasticsearch.com/docs/elasticsearch/rest_api/search/
    def search(query, options = {})
      page = (options.delete(:page) || 1).to_i
      options[:fields] ||= '_id'
      options[:size] ||= per_page_for_search(options)
      options[:from] ||= options[:size] * (page - 1)
      if query.is_a?(Hash)
        options[:query] = query
      else
        options[:query] = {
          :query_string => {
            :query => query,
            :default_operator => options.delete(:default_operator)
          }
        }
      end
      query = {}
      case sort = options.delete(:sort)
      when Array,Hash
        options[:sort] = sort
      when String
        query[:sort] = sort
      end

      options[:facets] = {}
      options[:facets].merge! term_facet_query_from(options.delete(:term_facets))
      options[:facets].merge! range_facet_query_from(options.delete(:range_facets))

      response = ElasticSearchable.request :get, index_type_path('_search'), :query => query, :json_body => options
      hits = response['hits']
      facets = response['facets']
      ids = hits['hits'].collect {|h| h['_id'].to_i }
      results = self.find(ids).sort_by {|result| ids.index(result.id) }

      page = WillPaginate::Collection.new(page, options[:size], hits['total'])
      page.replace results

      {
        :results => page,
        :request => ElasticSearchable.encode_json(options),
        :facets => facets_response_from(facets)
      }
    end

    private

    # determine the number of search results per page
    # supports will_paginate configuration by using:
    # Model.per_page
    # Model.max_per_page
    def per_page_for_search(options = {})
      per_page = options.delete(:per_page) || (self.respond_to?(:per_page) ? self.per_page : nil) || ElasticSearchable::Queries::PER_PAGE_DEFAULT
      per_page = [per_page.to_i, self.max_per_page].min if self.respond_to?(:max_per_page)
      per_page
    end

    def term_facet_query_from request
      return {} if request.nil?
      request.inject({}) do |hash, attr_hash|
        field = attr_hash.keys.first
        hash.merge({
          field => {
            :terms => {
              :field => field,
              :size => attr_hash[field]
            }
          }
        })
      end
    end

    def range_facet_query_from request
      return {} if request.nil?
      request.inject({}) do |hash, attr_hash|
        field = attr_hash.keys.first
        ranges = attr_hash[field].map do |range_string|
          from, to = range_string.split('|')
          {}.tap do |range|
            range.merge!({ :from => from }) unless from.blank?
            range.merge!({ :to => to }) unless to.blank?
          end
        end

        hash.merge({
          field => {
            :range => {
              :field => field,
              :ranges => ranges
            }
          }
        })
      end
    end

    def facets_response_from facets
      return {} if facets.nil?
      facets.keys.inject({}) do |hash, field|
        if facets[field]['terms']
          hash.merge(term_facet_response_for_field(facets, field))
        elsif facets[field]['ranges']
          hash.merge(range_facet_response_for_field(facets, field))
        end
      end
    end

    def term_facet_response_for_field facets, field
      counts = facets[field]['terms'].map do |term_response|
        { term_response['term'] => term_response['count'] }
      end

      { field.to_sym => {
          :counts => counts,
          :missing => facets[field]['missing'],
          :other => facets[field]['other']
        }
      }
    end

    def range_facet_response_for_field facets, field
      counts = facets[field]['ranges'].map do |range_response|
        { "#{range_response['from_str']}|#{range_response['to_str']}" => range_response['count'] }
      end

      { field.to_sym => {
          :counts => counts
        }
      }
    end
  end
end
