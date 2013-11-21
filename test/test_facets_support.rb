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
         :name => { :type => :string, :index => :not_analyzed },
         :number => { :type => :integer, :index => :not_analyzed },
         :last_used => { :type => :date, :index => :not_analyzed, :format => "yyyy/MM/dd HH:mm:ss Z" }
        }
      }
  end

  context "facets" do
    context "term_facets" do
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
        facets = Thing.search('stuff', { :term_facets => [{:title => 5}] })[:facets]
        assert_equal 1, facets.keys.size
        assert_equal({ 'AA' => 3 }, facets[:title][:counts][0])
        assert_equal({ 'DD' => 2 }, facets[:title][:counts][1])
        assert_equal({ 'EE' => 1 }, facets[:title][:counts][2])
        assert_equal({ 'CC' => 1 }, facets[:title][:counts][3])
        assert_equal({ 'BB' => 1 }, facets[:title][:counts][4])
      end

      should 'return counts for multiple facets' do
        facets = Thing.search('stuff', { :term_facets => [{:title => 5}, {:name => 5}] })[:facets]
        assert_equal 2, facets.keys.size
        assert_equal 5, facets[:title][:counts].size
        assert_equal 1, facets[:name][:counts].size
      end

      should 'show the count of missing' do
        facets = Thing.search('stuff', { :term_facets => [{:name => 5}] })[:facets]
        assert_equal 1, facets.keys.size
        assert_equal(6, facets[:name][:missing])
      end

      should 'show the count of other (when the number exceeds the request number of facets' do
        facets = Thing.search('stuff', { :term_facets => [{ :title => 3}] })[:facets]
        assert_equal 1, facets.keys.size
        assert_equal 2, facets[:title][:other]
      end
    end

    context 'range_facets' do
      setup do
        Thing.create_index
        Thing.create! :number => 0, :last_used => 1.second.ago
        Thing.create! :number => 1, :last_used => 3.hours.ago
        Thing.create! :number => 2, :last_used => 1.day.ago - 1.second
        Thing.create! :number => 2, :last_used => 2.days.ago
        Thing.create! :number => 3, :last_used => 3.days.ago
        Thing.create! :number => 4, :last_used => 4.days.ago
        Thing.create! :number => 5, :last_used => 5.days.ago
        Thing.create! :number => 6, :last_used => 6.days.ago
        Thing.create! :number => 7, :last_used => 7.days.ago
        Thing.refresh_index
      end

      should 'return counts for each discrete range (upper bound not included)' do
        facets = Thing.search({:match_all=>{}}, { :range_facets => [ { :number => ["1|3", "3|5"] } ] })[:facets]

        assert_equal 1, facets.keys.size
        assert_equal({ "1|3" => 3 }, facets[:number][:counts][0])
        assert_equal({ "3|5" => 2 }, facets[:number][:counts][1])
      end

      should 'allow only specifying the lower bound' do
        facets = Thing.search({:match_all=>{}}, { :range_facets => [ { :number => ["0|3", "3|"] } ] })[:facets]

        assert_equal 1, facets.keys.size
        assert_equal({ "0|3" => 4 }, facets[:number][:counts][0])
        assert_equal({ "3|"  => 5 }, facets[:number][:counts][1])
      end

      should 'allow only specifying the upper bound' do
        facets = Thing.search({:match_all=>{}}, { :range_facets => [ { :number => ["|3", "3|5"] } ] })[:facets]

        assert_equal 1, facets.keys.size
        assert_equal({ "|3" => 4 }, facets[:number][:counts][0])
        assert_equal({ "3|5"  => 2 }, facets[:number][:counts][1])
      end

      should 'allow time ranges' do
        from = 1.day.ago.strftime("%Y/%m/%d %H:%M:%S -0500")
        to = Time.now.strftime("%Y/%m/%d %H:%M:%S -0500")
        facets = Thing.search({:match_all=>{}}, { :range_facets => [ { :last_used => [ "#{from}|#{to}"] } ] })[:facets]

        assert_equal 1, facets.keys.size
        assert_equal({ "#{from}|#{to}" => 2 }, facets[:last_used][:counts][0])
      end
    end

    context 'combinations' do
      setup do
        Thing.create_index
        Thing.create! :number => 0, :title => 'AB'
        Thing.create! :number => 1, :title => 'AB'
        Thing.create! :number => 2, :title => 'AB'
        Thing.create! :number => 2, :title => 'AB'
        Thing.create! :number => 3, :title => 'AC'
        Thing.create! :number => 4, :title => 'AC'
        Thing.create! :number => 5, :title => 'AC'
        Thing.create! :number => 6, :title => 'AA'
        Thing.create! :number => 7, :title => 'AA'
        Thing.create! :number => 8, :title => 'AD'
        Thing.refresh_index
      end

      should 'allow both types in same quer' do
        facets = Thing.search({:match_all=>{}}, { :range_facets => [ { :number => ["1|3", "3|5"] } ], :term_facets => [{:title => 3}] })[:facets]

        assert_equal 2, facets.keys.size
        assert_equal 2, facets[:number][:counts].size
        assert_equal({ "1|3" => 3 }, facets[:number][:counts][0])
        assert_equal({ "3|5" => 2 }, facets[:number][:counts][1])

        assert_equal 3, facets[:title][:counts].size
        assert_equal({ "AB" => 4 }, facets[:title][:counts][0])
        assert_equal({ "AC" => 3 }, facets[:title][:counts][1])
        assert_equal({ "AA" => 2 }, facets[:title][:counts][2])
      end
    end
  end
end

