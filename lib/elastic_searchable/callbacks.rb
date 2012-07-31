module ElasticSearchable
  module Callbacks
    module InstanceMethods
      private
      def delete_from_index
        self.class.delete_id_from_index self.id
      end
      def update_index_on_create
        reindex :create
      end
      def update_index_on_update
        reindex :update
      end
    end
  end
end
