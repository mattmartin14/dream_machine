/*!
 * Copyright 2019 Google Inc. All Rights Reserved.
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
import { ServiceObject } from '@google-cloud/common';
import { BigQuery, Job, Dataset, ResourceCallback, RequestCallback, JobRequest } from '.';
import { JobMetadata } from './job';
import bigquery from './types';
export interface File {
    bucket: any;
    kmsKeyName?: string;
    userProject?: string;
    name: string;
    generation?: number;
}
export type JobMetadataCallback = RequestCallback<JobMetadata>;
export type JobMetadataResponse = [JobMetadata];
export type JobResponse = [Job, bigquery.IJob];
export type JobCallback = ResourceCallback<Job, bigquery.IJob>;
export type CreateExtractJobOptions = JobRequest<bigquery.IJobConfigurationExtract> & {
    format?: 'ML_TF_SAVED_MODEL' | 'ML_XGBOOST_BOOSTER';
};
/**
 * Model objects are returned by methods such as {@link Dataset#model} and
 * {@link Dataset#getModels}.
 *
 * @class
 * @param {Dataset} dataset {@link Dataset} instance.
 * @param {string} id The ID of the model.
 *
 * @example
 * ```
 * const {BigQuery} = require('@google-cloud/bigquery');
 * const bigquery = new BigQuery();
 * const dataset = bigquery.dataset('my-dataset');
 *
 * const model = dataset.model('my-model');
 * ```
 */
declare class Model extends ServiceObject {
    dataset: Dataset;
    bigQuery: BigQuery;
    constructor(dataset: Dataset, id: string);
    /**
     * @callback JobCallback
     * @param {?Error} err Request error, if any.
     * @param {object} Job The [Job]{@link https://cloud.google.com/bigquery/docs/reference/v2/Job} resource.
     * @param {object} apiResponse The full API response.
     */
    /**
     * @typedef {array} JobResponse
     * @property {object} 0 The [Job]{@link https://cloud.google.com/bigquery/docs/reference/v2/Job} resource.
     * @property {object} 1 The full API response.
     */
    /**
     * Export model to Cloud Storage.
     *
     * See {@link https://cloud.google.com/bigquery/docs/reference/v2/jobs/insert| Jobs: insert API Documentation}
     *
     * @param {string|File} destination Where the model should be exported
     *    to. A string or {@link
     *    https://googleapis.dev/nodejs/storage/latest/File.html File}
     *    object.
     * @param {object} [options] The configuration object. For all extract job options, see [CreateExtractJobOptions]{@link https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationExtract}.
     * @param {string} [options.format] The format to export the data in.
     *    Allowed options are "ML_TF_SAVED_MODEL" or "ML_XGBOOST_BOOSTER".
     *    Default: "ML_TF_SAVED_MODEL".
     * @param {string} [options.jobId] Custom job id.
     * @param {string} [options.jobPrefix] Prefix to apply to the job id.
     * @param {JobCallback} [callback] The callback function.
     * @param {?error} callback.err An error returned while making this request.
     * @param {Job} callback.job The job used to export the model.
     * @param {object} callback.apiResponse The full API response.
     * @returns {Promise<JobResponse>}
     *
     * @throws {Error} If a destination isn't a string or File object.
     *
     * @example
     * ```
     * const {BigQuery} = require('@google-cloud/bigquery');
     * const bigquery = new BigQuery();
     * const dataset = bigquery.dataset('my-dataset');
     * const model = dataset.model('my-model');
     *
     * const extractedModel = 'gs://my-bucket/extracted-model';
     *
     * function callback(err, job, apiResponse) {
     *   // `job` is a Job object that can be used to check the status of the
     *   // request.
     * }
     *
     * //-
     * // To use the default options, just pass a string or a {@link
     * https://googleapis.dev/nodejs/storage/latest/File.html File}
     * object.
     * //
     * // Note: The default format is 'ML_TF_SAVED_MODEL'.
     * //-
     * model.createExtractJob(extractedModel, callback);
     *
     * //-
     * // If you need more customization, pass an `options` object.
     * //-
     * const options = {
     *   format: 'ML_TF_SAVED_MODEL',
     *   jobId: '123abc'
     * };
     *
     * model.createExtractJob(extractedModel, options, callback);
     *
     * //-
     * // If the callback is omitted, we'll return a Promise.
     * //-
     * model.createExtractJob(extractedModel, options).then((data) => {
     *   const job = data[0];
     *   const apiResponse = data[1];
     * });
     * ```
     */
    createExtractJob(destination: string | File, options?: CreateExtractJobOptions): Promise<JobResponse>;
    createExtractJob(destination: string | File, options: CreateExtractJobOptions, callback: JobCallback): void;
    createExtractJob(destination: string | File, callback: JobCallback): void;
    /**
     * @callback JobMetadataCallback
     * @param {?Error} err Request error, if any.
     * @param {object} metadata The job metadata.
     * @param {object} apiResponse The full API response.
     */
    /**
     * @typedef {array} JobMetadataResponse
     * @property {object} 0 The job metadata.
     * @property {object} 1 The full API response.
     */
    /**
     * Export model to Cloud Storage.
     *
     * @param {string|File} destination Where the model should be exported
     *    to. A string or {@link
     *    https://googleapis.dev/nodejs/storage/latest/File.html File}
     *    object.
     * @param {object} [options] The configuration object. For all extract job options, see [CreateExtractJobOptions]{@link https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationExtract}.
     * @param {string} [options.format] The format to export
     *    the data in. Allowed options are "ML_TF_SAVED_MODEL" or
     *    "ML_XGBOOST_BOOSTER". Default: "ML_TF_SAVED_MODEL".
     * @param {string} [options.jobId] Custom id for the underlying job.
     * @param {string} [options.jobPrefix] Prefix to apply to the underlying job id.
     * @param {JobMetadataCallback} [callback] The callback function.
     * @param {?error} callback.err An error returned while making this request
     * @param {object} callback.apiResponse The full API response.
     * @returns {Promise<JobMetadataResponse>}
     *
     * @throws {Error} If destination isn't a string or File object.
     *
     * @example
     * ```
     * const {BigQuery} = require('@google-cloud/bigquery');
     * const bigquery = new BigQuery();
     * const dataset = bigquery.dataset('my-dataset');
     * const model = dataset.model('my-model');
     *
     * const extractedModel = 'gs://my-bucket/extracted-model';
     *
     *
     * //-
     * function callback(err, job, apiResponse) {
     *   // `job` is a Job object that can be used to check the status of the
     *   // request.
     * }
     *
     * //-
     * // To use the default options, just pass a string or a {@link
     * https://googleapis.dev/nodejs/storage/latest/File.html File}
     * object.
     * //
     * // Note: The default format is 'ML_TF_SAVED_MODEL'.
     * //-
     * model.createExtractJob(extractedModel, callback);
     *
     * //-
     * // If you need more customization, pass an `options` object.
     * //-
     * const options = {
     *   format: 'ML_TF_SAVED_MODEL',
     *   jobId: '123abc'
     * };
     *
     * model.createExtractJob(extractedModel, options, callback);
     *
     * //-
     * // If the callback is omitted, we'll return a Promise.
     * //-
     * model.createExtractJob(extractedModel, options).then((data) => {
     *   const job = data[0];
     *   const apiResponse = data[1];
     * });
     * ```
     */
    extract(destination: string | File, options?: CreateExtractJobOptions): Promise<JobMetadataResponse>;
    extract(destination: string | File, options: CreateExtractJobOptions, callback?: JobMetadataCallback): void;
    extract(destination: string | File, callback?: JobMetadataCallback): void;
}
/**
 * Reference to the {@link Model} class.
 * @name module:@google-cloud/bigquery.Model
 * @see Model
 */
export { Model };
