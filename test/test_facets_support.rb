require File.join(File.dirname(__FILE__), 'helper')

class TestElasticSearchable < Test::Unit::TestCase
  def setup
    delete_index
  end
  ElasticSearchable.debug_output

  class Thing < ActiveRecord::Base
    elastic_searchable :index_options => {'number_of_replicas' => 0, 'number_of_shards' => 1},
      :mapping => {
        :properties => {
         :title => { :type => :string, :index => :not_analyzed },
         :name => { :type => :string, :index => :not_analyzed }
        }
      }
  end

  context "facets" do
    setup do
      Thing.create_index
      Thing.create! :title => 'AA', :body => 'more stuff', :name => 'Foo'
      Thing.create! :title => 'BB', :body => 'some stuff', :name => 'Foo'
      Thing.create! :title => 'AA', :body => 'yet more stuff'
      Thing.refresh_index
    end

    should 'return counts for each item' do
      facets = Thing.search('stuff', { :facets => [:title] })[:facets]
      assert_equal 1, facets.keys.size
      assert_equal({ 'AA' => 2 }, facets[:title][0])
      assert_equal({ 'BB' => 1 }, facets[:title][1])
    end

    should 'return counts for multiple facets' do
      facets = Thing.search('stuff', { :facets => [:title, :name] })[:facets]
      assert_equal 2, facets.keys.size
      assert_equal 2, facets[:title].size
      assert_equal 1, facets[:name].size
    end

    should 'return counts for multiple facets' do
      facets = Thing.search('stuff', { :facets => [:name] })[:facets]
      assert_equal 1, facets.keys.size
      assert_equal({ 'Foo' => 2 }, facets[:name][0])
      assert_equal({ nil => 1 }, facets[:name][1])
    end
  end
end

