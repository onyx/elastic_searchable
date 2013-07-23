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
      Thing.create! :title => 'AA', :body => 'and more stuff'
      Thing.create! :title => 'DD', :body => 'more stuff'
      Thing.create! :title => 'DD', :body => 'yet more stuff'
      Thing.create! :title => 'CC', :body => 'more stuff'
      Thing.create! :title => 'EE', :body => 'more stuff'
      Thing.refresh_index
    end

    should 'return counts for each item' do
      facets = Thing.search('stuff', { :facets => [{:title => 5}] })[:facets]
      assert_equal 1, facets.keys.size
      assert_equal({ 'AA' => 3 }, facets[:title][:counts][0])
      assert_equal({ 'DD' => 2 }, facets[:title][:counts][1])
      assert_equal({ 'EE' => 1 }, facets[:title][:counts][2])
      assert_equal({ 'CC' => 1 }, facets[:title][:counts][3])
      assert_equal({ 'BB' => 1 }, facets[:title][:counts][4])
    end

    should 'return counts for multiple facets' do
      facets = Thing.search('stuff', { :facets => [{:title => 5}, {:name => 5}] })[:facets]
      assert_equal 2, facets.keys.size
      assert_equal 5, facets[:title][:counts].size
      assert_equal 1, facets[:name][:counts].size
    end

    should 'show the count of missing' do
      facets = Thing.search('stuff', { :facets => [{:name => 5}] })[:facets]
      assert_equal 1, facets.keys.size
      assert_equal(6, facets[:name][:missing])
    end

    should 'show the count of other (when the number exceeds the request number of facets' do
      facets = Thing.search('stuff', { :facets => [{ :title => 3}] })[:facets]
      assert_equal 1, facets.keys.size
      assert_equal 2, facets[:title][:other]
    end
  end
end

