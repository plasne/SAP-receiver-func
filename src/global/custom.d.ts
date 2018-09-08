
import * as azs from "azure-storage";

// support null for continuation token in TS
declare module "azure-storage" {
    module services {
        module blob {
            module blobservice {
                interface BlobService {
                    listBlobsSegmented(container: string, currentToken: common.ContinuationToken | null, callback: ErrorOrResult<BlobService.ListBlobsResult>): void;
                }
            }
        }
    }
}