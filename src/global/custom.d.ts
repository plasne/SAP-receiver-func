import * as azs from 'azure-storage';

// support null for continuation token in TS
declare module 'azure-storage' {
    namespace services {
        namespace blob {
            namespace blobservice {
                interface BlobService {
                    listBlobsSegmented(
                        container: string,
                        currentToken:
                            | common.ContinuationToken
                            | null
                            | undefined,
                        callback: ErrorOrResult<BlobService.ListBlobsResult>
                    ): void;

                    listBlobsSegmentedWithPrefix(
                        container: string,
                        prefix: string,
                        currentToken:
                            | common.ContinuationToken
                            | null
                            | undefined,
                        callback: ErrorOrResult<BlobService.ListBlobsResult>
                    ): void;
                }
            }
        }
    }
}

// support null for continuation token in TS
declare module 'azure-storage' {
    namespace services {
        namespace table {
            interface TableService {
                queryEntities<T>(
                    table: string,
                    query: TableQuery,
                    currentToken:
                        | TableService.TableContinuationToken
                        | null
                        | undefined,
                    callback: ErrorOrResult<TableService.QueryEntitiesResult<T>>
                ): void;
            }
        }
    }
}

// queued message format
type message = {
    partition: string;
    filenames: string[];
};
