export interface ISchemaColumn {
    header: string;
    path: string;
    default?: string;
    enclosure?: string;
}

export interface ISchema {
    name: string;
    filename: string;
    identifier: string;
    selector: string;
    columns: ISchemaColumn[];
}
