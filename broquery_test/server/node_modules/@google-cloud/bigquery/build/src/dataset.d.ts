/*!
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { DeleteCallback, Metadata, ServiceObject } from '@google-cloud/common';
import { ResourceStream } from '@google-cloud/paginator';
import { Duplex } from 'stream';
import { BigQuery, PagedCallback, PagedRequest, PagedResponse, Query, QueryRowsResponse, ResourceCallback, SimpleQueryRowsCallback } from './bigquery';
import { JobCallback, JobResponse, Table, TableMetadata, TableOptions } from './table';
import { Model } from './model';
import { Routine } from './routine';
import bigquery from './types';
export interface DatasetDeleteOptions {
    force?: boolean;
}
export interface DatasetOptions {
    location?: string;
    projectId?: string;
}
export type CreateDatasetOptions = bigquery.IDataset;
export type GetModelsOptions = PagedRequest<bigquery.models.IListParams>;
export type GetModelsResponse = PagedResponse<Model, GetModelsOptions, bigquery.IListModelsResponse>;
export type GetModelsCallback = PagedCallback<Model, GetModelsOptions, bigquery.IListModelsResponse>;
export type GetRoutinesOptions = PagedRequest<bigquery.routines.IListParams>;
export type GetRoutinesResponse = PagedResponse<Routine, GetRoutinesOptions, bigquery.IListRoutinesResponse>;
export type GetRoutinesCallback = PagedCallback<Routine, GetRoutinesOptions, bigquery.IListRoutinesResponse>;
export type GetTablesOptions = PagedRequest<bigquery.tables.IListParams>;
export type GetTablesResponse = PagedResponse<Table, GetTablesOptions, bigquery.ITableList>;
export type GetTablesCallback = PagedCallback<Table, GetTablesOptions, bigquery.ITableList>;
export type RoutineMetadata = bigquery.IRoutine;
export type RoutineResponse = [Routine, bigquery.IRoutine];
export type RoutineCallback = ResourceCallback<Routine, bigquery.IRoutine>;
export type TableResponse = [Table, bigquery.ITable];
export type TableCallback = ResourceCallback<Table, bigquery.ITable>;
/**
 * Interact with your BigQuery dataset. Create a Dataset instance with
 * {@link BigQuery#createDataset} or {@link BigQuery#dataset}.
 *
 * @class
 * @param {BigQuery} bigQuery {@link BigQuery} instance.
 * @param {string} id The ID of the Dataset.
 * @param {object} [options] Dataset options.
 * @param {string} [options.projectId] The GCP project ID.
 * @param {string} [options.location] The geographic location of the dataset.
 *      Defaults to US.
 *
 * @example
 * ```
 * const {BigQuery} = require('@google-cloud/bigquery');
 * const bigquery = new BigQuery();
 * const dataset = bigquery.dataset('institutions');
 * ```
 */
declare class Dataset extends ServiceObject {
    bigQuery: BigQuery;
    location?: string;
    projectId: string;
    getModelsStream(options?: GetModelsOptions): ResourceStream<Model>;
    getRoutinesStream(options?: GetRoutinesOptions): ResourceStream<Routine>;
    getTablesStream(options?: GetTablesOptions): ResourceStream<Table>;
    constructor(bigQuery: BigQuery, id: string, options?: DatasetOptions);
    /**
     * Run a query as a job. No results are immediately returned. Instead, your
     * callback will be executed with a {@link Job} object that you must
     * ping for the results. See the Job documentation for explanations of how to
     * check on the status of the job.
     *
     * See {@link BigQuery#createQueryJob} for full documentation of this method.
     *
     * @param {object} options See {@link BigQuery#createQueryJob} for full documentation of this method.
     * @param {JobCallback} [callback] See {@link BigQuery#createQueryJob} for full documentation of this method.
     * @returns {Promise<JobResponse>} See {@link BigQuery#createQueryJob} for full documentation of this method.
     */
    createQueryJob(options: string | Query): Promise<JobResponse>;
    createQueryJob(options: string | Query, callback: JobCallback): void;
    /**
     * Run a query scoped to your dataset as a readable object stream.
     *
     * See {@link BigQuery#createQueryStream} for full documentation of this
     * method.
     *
     * @param {object} options See {@link BigQuery#createQueryStream} for full
     *     documentation of this method.
     * @returns {stream}
     */
    createQueryStream(options: Query | string): Duplex;
    /**
     * @callback CreateRoutineCallback
     * @param {?Error} err Request error, if any.
     * @param {Routine} routine The newly created routine.
     * @param {object} response The full API response body.
     */
    /**
     * @typedef {array} CreateRoutineResponse
     * @property {Routine} 0 The newly created routine.
     * @property {object} 1 The full API response body.
     */
    /**
     * Create a {@link Routine}.
     *
     * See {@link https://cloud.google.com/bigquery/docs/reference/rest/v2/routines/insert| Routines: insert API Documentation}
     *
     * @param {string} id The routine ID.
     * @param {object} config A [routine resource]{@link https://cloud.google.com/bigquery/docs/reference/rest/v2/routines#Routine}.
     * @param {CreateRoutineCallback} [callback] The callback function.
     * @returns {Promise<CreateRoutineResponse>}
     *
     * @example
     * ```
     * const {BigQuery} = require('@google-cloud/bigquery');
     * const bigquery = new BigQuery();
     * const dataset = bigquery.dataset('my-dataset');
     *
     * const id = 'my-routine';
     * const config = {
     *   arguments: [{
     *     name: 'x',
     *     dataType: {
     *       typeKind: 'INT64'
     *     }
     *   }],
     *   definitionBody: 'x * 3',
     *   routineType: 'SCALAR_FUNCTION',
     *   returnType: {
     *     typeKind: 'INT64'
     *   }
     * };
     *
     * dataset.createRoutine(id, config, (err, routine, apiResponse) => {
     *   if (!err) {
     *     // The routine was created successfully.
     *   }
     * });
     *
     * ```
     * @example If the callback is omitted a Promise will be returned
     * ```
     * const [routine, apiResponse] = await dataset.createRoutine(id, config);
     * ```
     */
    createRoutine(id: string, config: RoutineMetadata): Promise<RoutineResponse>;
    createRoutine(id: string, config: RoutineMetadata, callback: RoutineCallback): void;
    /**
     * @callback TableCallback
     * @param {?Error} err Request error, if any.
     * @param {Table} table The table.
     * @param {object} apiResponse The full API response body.
     */
    /**
     * @typedef {array} TableResponse
     * @property {Table} 0 The table.
     * @property {object} 1 The full API response body.
     */
    /**
     * Create a {@link Table} given a tableId or configuration object.
     *
     * See {@link https://cloud.google.com/bigquery/docs/reference/v2/tables/insert| Tables: insert API Documentation}
     *
     * @param {string} id Table id.
     * @param {object} [options] See a
     *     {@link https://cloud.google.com/bigquery/docs/reference/v2/tables#resource| Table resource}.
     * @param {string|object} [options.schema] A comma-separated list of name:type
     *     pairs. Valid types are "string", "integer", "float", "boolean", and
     *     "timestamp". If the type is omitted, it is assumed to be "string".
     *     Example: "name:string, age:integer". Schemas can also be specified as a
     *     JSON array of fields, which allows for nested and repeated fields. See
     *     a {@link http://goo.gl/sl8Dmg| Table resource} for more detailed information.
     * @param {TableCallback} [callback] The callback function.
     * @param {?error} callback.err An error returned while making this request
     * @param {Table} callback.table The newly created table.
     * @param {object} callback.apiResponse The full API response.
     * @returns {Promise<TableResponse>}
     *
     * @example
     * ```
     * const {BigQuery} = require('@google-cloud/bigquery');
     * const bigquery = new BigQuery();
     * const dataset = bigquery.dataset('institutions');
     *
     * const tableId = 'institution_data';
     *
     * const options = {
     *   // From the data.gov CSV dataset (http://goo.gl/kSE7z6):
     *   schema: 'UNITID,INSTNM,ADDR,CITY,STABBR,ZIP,FIPS,OBEREG,CHFNM,...'
     * };
     *
     * dataset.createTable(tableId, options, (err, table, apiResponse) => {});
     *
     * //-
     * // If the callback is omitted, we'll return a Promise.
     * //-
     * dataset.createTable(tableId, options).then((data) => {
     *   const table = data[0];
     *   const apiResponse = data[1];
     * });
     * ```
     */
    createTable(id: string, options: TableMetadata): Promise<TableResponse>;
    createTable(id: string, options: TableMetadata, callback: TableCallback): void;
    createTable(id: string, callback: TableCallback): void;
    /**
     * @callback DeleteCallback
     * @param {?Error} err Request error, if any.
     * @param {object} apiResponse The full API response body.
     */
    /**
     * @typedef {array} Metadata
     * @property {object} 0 The full API response body.
     */
    /**
     * Delete the dataset.
     *
     * See {@link https://cloud.google.com/bigquery/docs/reference/v2/datasets/delete| Datasets: delete API Documentation}
     *
     * @param {object} [options] The configuration object.
     * @param {boolean} [options.force=false] Force delete dataset and all tables.
     * @param {DeleteCallback} [callback] The callback function.
     * @param {?error} callback.err An error returned while making this request
     * @param {object} callback.apiResponse The full API response.
     * @returns {Promise<Metadata>}
     *
     * @example
     * ```
     * const {BigQuery} = require('@google-cloud/bigquery');
     * const bigquery = new BigQuery();
     * const dataset = bigquery.dataset('institutions');
     *
     * //-
     * // Delete the dataset, only if it does not have any tables.
     * //-
     * dataset.delete((err, apiResponse) => {});
     *
     * //-
     * // Delete the dataset and any tables it contains.
     * //-
     * dataset.delete({ force: true }, (err, apiResponse) => {});
     *
     * //-
     * // If the callback is omitted, we'll return a Promise.
     * //-
     * dataset.delete().then((data) => {
     *   const apiResponse = data[0];
     * });
     * ```
     */
    delete(options?: DatasetDeleteOptions): Promise<[Metadata]>;
    delete(options: DatasetDeleteOptions, callback: DeleteCallback): void;
    delete(callback: DeleteCallback): void;
    /**
     * @typedef {object} GetModelsOptions
     * @property {boolean} [autoPaginate=true] Have pagination handled
     *     automatically.
     * @property {number} [maxApiCalls] Maximum number of API calls to make.
     * @property {number} [maxResults] Maximum number of results to return.
     * @property {string} [pageToken] Token returned from a previous call, to
     *     request the next page of results.
     */
    /**
     * @callback GetModelsCallback
     * @param {?Error} err Request error, if any.
     * @param {Model[]} models List of model objects.
     * @param {GetModelsOptions} nextQuery If `autoPaginate` is set to true,
     *     this will be a prepared query for the next page of results.
     * @param {object} response The full API response.
     */
    /**
     * @typedef {array} GetModelsResponse
     * @property {Model[]} 0 A list of the dataset's {@link Model} objects.
     * @property {GetModelsOptions} 1 If `autoPaginate` is set to true, this
     *     will be a prepared query for the next page of results.
     * @property {object} 2 The full API response.
     */
    /**
     * Get a list of {@link Model} resources.
     *
     * See {@link https://cloud.google.com/bigquery/docs/reference/rest/v2/models/list| Models: list API Documentation}
     *
     * @param {GetModelsOptions} [options] Configuration object.
     * @param {boolean} [options.autoPaginate=true] Have pagination handled
     *     automatically.
     * @param {number} [options.maxApiCalls] Maximum number of API calls to make.
     * @param {number} [options.maxResults] Maximum number of results to return.
     * @param {string} [options.pageToken] Token returned from a previous call, to
     *     request the next page of results.
     * @param {GetModelsCallback} [callback] The callback function.
     * @param {?error} callback.err An error returned while making this request
     * @param {Model[]} callback.models The list of models from
     *     your Dataset.
     * @param {GetModelsOptions} callback.nextQuery If `autoPaginate` is set to true, this
     *     will be a prepared query for the next page of results.
     * @param {object} callback.apiResponse The full API response.
     * @returns {Promise<GetModelsResponse>}
     *
     * @example
     * ```
     * const {BigQuery} = require('@google-cloud/bigquery');
     * const bigquery = new BigQuery();
     * const dataset = bigquery.dataset('institutions');
     *
     * dataset.getModels((err, models) => {
     *   // models is an array of `Model` objects.
     * });
     *
     * ```
     * @example To control how many API requests are made and page through the results manually, set `autoPaginate` to `false`.
     * ```
     * function manualPaginationCallback(err, models, nextQuery, apiResponse) {
     *   if (nextQuery) {
     *     // More results exist.
     *     dataset.getModels(nextQuery, manualPaginationCallback);
     *   }
     * }
     *
     * dataset.getModels({
     *   autoPaginate: false
     * }, manualPaginationCallback);
     *
     * ```
     * @example If the callback is omitted, we'll return a Promise.
     * ```
     * dataset.getModels().then((data) => {
     *   const models = data[0];
     * });
     * ```
     */
    getModels(options?: GetModelsOptions): Promise<GetModelsResponse>;
    getModels(options: GetModelsOptions, callback: GetModelsCallback): void;
    getModels(callback: GetModelsCallback): void;
    /**
     * @typedef {object} GetRoutinesOptions
     * @property {boolean} [autoPaginate=true] Have pagination handled
     *     automatically.
     * @property {number} [maxApiCalls] Maximum number of API calls to make.
     * @property {number} [maxResults] Maximum number of results to return.
     * @property {string} [pageToken] Token returned from a previous call, to
     *     request the next page of results.
     */
    /**
     * @callback GetRoutinesCallback
     * @param {?Error} err Request error, if any.
     * @param {Routine[]} routines List of routine objects.
     * @param {GetRoutinesOptions} nextQuery If `autoPaginate` is set to true,
     *     this will be a prepared query for the next page of results.
     * @param {object} response The full API response.
     */
    /**
     * @typedef {array} GetRoutinesResponse
     * @property {Routine[]} 0 List of {@link Routine} objects.
     * @property {GetRoutinesOptions} 1 If `autoPaginate` is set to true, this
     *     will be a prepared query for the next page of results.
     * @property {object} 2 The full API response.
     */
    /**
     * Get a list of routines.
     *
     * See {@link https://cloud.google.com/bigquery/docs/reference/rest/v2/routines/list| Routines: list API Documentation}
     *
     * @param {GetRoutinesOptions} [options] Request options.
     * @param {boolean} [options.autoPaginate=true] Have pagination handled
     *     automatically.
     * @param {number} [options.maxApiCalls] Maximum number of API calls to make.
     * @param {number} [options.maxResults] Maximum number of results to return.
     * @param {string} [options.pageToken] Token returned from a previous call, to
     *     request the next page of results.
     * @param {GetRoutinesCallback} [callback] The callback function.
     * @param {?error} callback.err An error returned while making this request
     * @param {Routine[]} callback.routines The list of models from
     *     your Dataset.
     * @param {GetRoutinesOptions} callback.nextQuery If `autoPaginate` is set to true, this
     *     will be a prepared query for the next page of results.
     * @param {object} callback.apiResponse The full API response.
     * @returns {Promise<GetRoutinesResponse>}
     *
     * @example
     * ```
     * const {BigQuery} = require('@google-cloud/bigquery');
     * const bigquery = new BigQuery();
     * const dataset = bigquery.dataset('institutions');
     *
     * dataset.getRoutines((err, routines) => {
     *   // routines is an array of `Routine` objects.
     * });
     *
     * ```
     * @example To control how many API requests are made and page through the results manually, set `autoPaginate` to `false`.
     * ```
     * function manualPaginationCallback(err, routines, nextQuery, apiResponse) {
     *   if (nextQuery) {
     *     // More results exist.
     *     dataset.getRoutines(nextQuery, manualPaginationCallback);
     *   }
     * }
     *
     * dataset.getRoutines({
     *   autoPaginate: false
     * }, manualPaginationCallback);
     *
     * ```
     * @example If the callback is omitted a Promise will be returned
     * ```
     * const [routines] = await dataset.getRoutines();
     * ```
     */
    getRoutines(options?: GetRoutinesOptions): Promise<GetRoutinesResponse>;
    getRoutines(options: GetRoutinesOptions, callback: GetRoutinesCallback): void;
    getRoutines(callback: GetRoutinesCallback): void;
    /**
     * @typedef {object} GetTablesOptions
     * @property {boolean} [autoPaginate=true] Have pagination handled
     *     automatically.
     * @property {number} [maxApiCalls] Maximum number of API calls to make.
     * @property {number} [maxResults] Maximum number of results to return.
     * @property {string} [pageToken] Token returned from a previous call, to
     *     request the next page of results.
     */
    /**
     * @callback GetTablesCallback
     * @param {?Error} err Request error, if any.
     * @param {Table[]} tables List of {@link Table} objects.
     * @param {GetTablesOptions} nextQuery If `autoPaginate` is set to true,
     *     this will be a prepared query for the next page of results.
     * @param {object} response The full API response.
     */
    /**
     * @typedef {array} GetTablesResponse
     * @property {Table[]} 0 List of {@link Table} objects.
     * @property {GetTablesOptions} 1 If `autoPaginate` is set to true, this
     *     will be a prepared query for the next page of results.
     * @property {object} 2 The full API response.
     */
    /**
     * Get a list of {@link Table} resources.
     *
     * See {@link https://cloud.google.com/bigquery/docs/reference/v2/tables/list| Tables: list API Documentation}
     *
     * @param {GetTablesOptions} options Configuration object.
     * @param {boolean} [options.autoPaginate=true] Have pagination handled automatically.
     * @param {number} [options.maxApiCalls] Maximum number of API calls to make.
     * @param {number} [options.maxResults] Maximum number of results to return.
     * @param {string} [options.pageToken] Token returned from a previous call, to
     *     request the next page of results.
     * @param {GetTablesCallback} [callback] The callback function.
     * @param {?error} callback.err An error returned while making this request
     * @param {Table[]} callback.tables The list of tables from
     *     your Dataset.
     * @param {GetTablesOptions} callback.nextQuery If `autoPaginate` is set to true, this
     *     will be a prepared query for the next page of results.
     * @param {object} callback.apiResponse The full API response.
     * @returns {Promise<GetTablesResponse>}
     *
     * @example
     * ```
     * const {BigQuery} = require('@google-cloud/bigquery');
     * const bigquery = new BigQuery();
     * const dataset = bigquery.dataset('institutions');
     *
     * dataset.getTables((err, tables) => {
     *   // tables is an array of `Table` objects.
     * });
     *
     * //-
     * // To control how many API requests are made and page through the results
     * // manually, set `autoPaginate` to `false`.
     * //-
     * function manualPaginationCallback(err, tables, nextQuery, apiResponse) {
     *   if (nextQuery) {
     *     // More results exist.
     *     dataset.getTables(nextQuery, manualPaginationCallback);
     *   }
     * }
     *
     * dataset.getTables({
     *   autoPaginate: false
     * }, manualPaginationCallback);
     *
     * //-
     * // If the callback is omitted, we'll return a Promise.
     * //-
     * dataset.getTables().then((data) => {
     *   const tables = data[0];
     * });
     * ```
     */
    getTables(options?: GetTablesOptions): Promise<GetTablesResponse>;
    getTables(options: GetTablesOptions, callback: GetTablesCallback): void;
    getTables(callback: GetTablesCallback): void;
    /**
     * Create a {@link Model} object.
     *
     * @throws {TypeError} if model ID is missing.
     *
     * @param {string} id The ID of the model.
     * @return {Model}
     *
     * @example
     * ```
     * const {BigQuery} = require('@google-cloud/bigquery');
     * const bigquery = new BigQuery();
     * const dataset = bigquery.dataset('institutions');
     *
     * const model = dataset.model('my-model');
     * ```
     */
    model(id: string): Model;
    /**
     * Run a query scoped to your dataset.
     *
     * See {@link BigQuery#query} for full documentation of this method.
     *
     * @param {object} options See {@link BigQuery#query} for full documentation of this method.
     * @param {function} [callback] See {@link BigQuery#query} for full documentation of this method.
     * @returns {Promise<SimpleQueryRowsResponse>}
     * @returns {Promise<QueryRowsResponse>} See {@link BigQuery#query} for full documentation of this method.
     */
    query(options: Query): Promise<QueryRowsResponse>;
    query(options: string): Promise<QueryRowsResponse>;
    query(options: Query, callback: SimpleQueryRowsCallback): void;
    query(options: string, callback: SimpleQueryRowsCallback): void;
    /**
     * Create a {@link Routine} object.
     *
     * @throws {TypeError} if routine ID is missing.
     *
     * @param {string} id The ID of the routine.
     * @returns {Routine}
     *
     * @example
     * ```
     * const {BigQuery} = require('@google-cloud/bigquery');
     * const bigquery = new BigQuery();
     * const dataset = bigquery.dataset('institutions');
     *
     * const routine = dataset.routine('my_routine');
     * ```
     */
    routine(id: string): Routine;
    /**
     * Create a {@link Table} object.
     *
     * @throws {TypeError} if table ID is missing.
     *
     * @param {string} id The ID of the table.
     * @param {object} [options] Table options.
     * @param {string} [options.location] The geographic location of the table, by
     *      default this value is inherited from the dataset. This can be used to
     *      configure the location of all jobs created through a table instance.
     * It cannot be used to set the actual location of the table. This value will
     *      be superseded by any API responses containing location data for the
     *      table.
     * @return {Table}
     *
     * @example
     * ```
     * const {BigQuery} = require('@google-cloud/bigquery');
     * const bigquery = new BigQuery();
     * const dataset = bigquery.dataset('institutions');
     *
     * const institutions = dataset.table('institution_data');
     * ```
     */
    table(id: string, options?: TableOptions): Table;
}
/**
 * Reference to the {@link Dataset} class.
 * @name module:@google-cloud/bigquery.Dataset
 * @see Dataset
 */
export { Dataset };
