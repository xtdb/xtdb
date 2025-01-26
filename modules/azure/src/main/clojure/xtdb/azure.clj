(ns xtdb.azure
  (:require [xtdb.buffer-pool :as bp]
            [xtdb.util :as util])
  (:import [xtdb.azure BlobStorage]))

(defmethod bp/->object-store-factory ::object-store [_ {:keys [storage-account container
                                                               prefix user-managed-identity-client-id
                                                               storage-account-endpoint]}]
  (cond-> (BlobStorage/azureBlobStorage storage-account container)
    prefix (.prefix (util/->path prefix))
    user-managed-identity-client-id (.userManagedIdentityClientId user-managed-identity-client-id)
    storage-account-endpoint (.storageAccountEndpoint storage-account-endpoint)))

